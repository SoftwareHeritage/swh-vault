# Copyright (C) 2016-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import io
import logging
import traceback
from typing import ClassVar, Optional, Set

from psycopg2.extensions import QueryCanceledError
import sentry_sdk

import swh.model.swhids
from swh.model.swhids import CoreSWHID, ObjectType
from swh.objstorage.interface import ObjStorageInterface
from swh.storage.interface import StorageInterface

MAX_BUNDLE_SIZE = 2**29  # 512 MiB
DEFAULT_CONFIG_PATH = "vault/cooker"
DEFAULT_CONFIG = {
    "max_bundle_size": ("int", MAX_BUNDLE_SIZE),
}


class PolicyError(Exception):
    """Raised when the bundle violates the cooking policy."""

    pass


class BundleTooLargeError(PolicyError):
    """Raised when the bundle is too large to be cooked."""

    pass


class BytesIOBundleSizeLimit(io.BytesIO):
    def __init__(self, *args, size_limit=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.size_limit = size_limit

    def write(self, chunk):
        if (
            self.size_limit is not None
            and self.getbuffer().nbytes + len(chunk) > self.size_limit
        ):
            raise BundleTooLargeError(
                "The requested bundle exceeds the maximum allowed "
                "size of {} bytes.".format(self.size_limit)
            )
        return super().write(chunk)


class BaseVaultCooker(metaclass=abc.ABCMeta):
    """Abstract base class for the vault's bundle creators

    This class describes a common API for the cookers.

    To define a new cooker, inherit from this class and override:
    - CACHE_TYPE_KEY: key to use for the bundle to reference in cache
    - def cook(): cook the object into a bundle
    """

    SUPPORTED_OBJECT_TYPES: ClassVar[Set[swh.model.swhids.ObjectType]]
    BUNDLE_TYPE: ClassVar[str]

    def __init__(
        self,
        swhid: CoreSWHID,
        backend,
        storage: StorageInterface,
        graph=None,
        objstorage: Optional[ObjStorageInterface] = None,
        max_bundle_size: int = MAX_BUNDLE_SIZE,
        thread_pool_size: int = 10,
    ):
        """Initialize the cooker.

        The type of the object represented by the id depends on the
        concrete class. Very likely, each type of bundle will have its
        own cooker class.

        Args:
            swhid: id of the object to be cooked into a bundle.
            backend: the vault backend (swh.vault.backend.VaultBackend).
        """
        self.check_object_type(swhid.object_type)
        self.swhid = swhid
        self.obj_id = swhid.object_id
        self.backend = backend
        self.storage = storage
        self.objstorage = objstorage
        self.graph = graph
        self.max_bundle_size = max_bundle_size
        self.thread_pool_size = thread_pool_size

    @classmethod
    def check_object_type(cls, object_type: ObjectType) -> None:
        if object_type not in cls.SUPPORTED_OBJECT_TYPES:
            raise ValueError(f"{cls.__name__} does not support {object_type} objects.")

    @abc.abstractmethod
    def check_exists(self):
        """Checks that the requested object exists and can be cooked.

        Override this in the cooker implementation.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def prepare_bundle(self):
        """Implementation of the cooker. Yields chunks of the bundle bytes.

        Override this with the cooker implementation.
        """
        raise NotImplementedError

    def cache_type_key(self) -> str:
        assert self.BUNDLE_TYPE
        return self.BUNDLE_TYPE

    def write(self, chunk):
        self.fileobj.write(chunk)

    def cook(self):
        """Cook the requested object into a bundle"""
        self.backend.set_status(self.BUNDLE_TYPE, self.swhid, "pending")
        self.backend.set_progress(self.BUNDLE_TYPE, self.swhid, "Processing...")

        self.fileobj = BytesIOBundleSizeLimit(size_limit=self.max_bundle_size)
        try:
            try:
                self.prepare_bundle()
            except QueryCanceledError:
                raise PolicyError(
                    "Timeout reached while assembling the requested bundle"
                )
            bundle = self.fileobj.getvalue()
            # TODO: use proper content streaming instead of put_bundle()
            self.backend.put_bundle(self.cache_type_key(), self.swhid, bundle)
        except PolicyError as e:
            logging.info("Bundle cooking violated policy: %s", e)
            self.backend.set_status(self.BUNDLE_TYPE, self.swhid, "failed")
            self.backend.set_progress(self.BUNDLE_TYPE, self.swhid, str(e))
        except Exception:
            self.backend.set_status(self.BUNDLE_TYPE, self.swhid, "failed")
            tb = traceback.format_exc()
            self.backend.set_progress(
                self.BUNDLE_TYPE,
                self.swhid,
                f"Internal Server Error. This incident will be reported.\n"
                f"The full error was:\n\n{tb}",
            )
            logging.exception("Bundle cooking failed.")
            sentry_sdk.capture_exception()
        else:
            self.backend.set_status(self.BUNDLE_TYPE, self.swhid, "done")
            self.backend.set_progress(self.BUNDLE_TYPE, self.swhid, None)
        finally:
            self.backend.send_notif(self.BUNDLE_TYPE, self.swhid)
