# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import io
import logging
from typing import Optional

from psycopg2.extensions import QueryCanceledError

from swh.model import hashutil

MAX_BUNDLE_SIZE = 2 ** 29  # 512 MiB
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

    CACHE_TYPE_KEY = None  # type: Optional[str]

    def __init__(
        self, obj_type, obj_id, backend, storage, max_bundle_size=MAX_BUNDLE_SIZE
    ):
        """Initialize the cooker.

        The type of the object represented by the id depends on the
        concrete class. Very likely, each type of bundle will have its
        own cooker class.

        Args:
            obj_type: type of the object to be cooked into a bundle (directory,
                      revision_flat or revision_gitfast; see
                      swh.vault.cooker.COOKER_TYPES).
            obj_id: id of the object to be cooked into a bundle.
            backend: the vault backend (swh.vault.backend.VaultBackend).
        """
        self.obj_type = obj_type
        self.obj_id = hashutil.hash_to_bytes(obj_id)
        self.backend = backend
        self.storage = storage
        self.max_bundle_size = max_bundle_size

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

    def write(self, chunk):
        self.fileobj.write(chunk)

    def cook(self):
        """Cook the requested object into a bundle
        """
        self.backend.set_status(self.obj_type, self.obj_id, "pending")
        self.backend.set_progress(self.obj_type, self.obj_id, "Processing...")

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
            self.backend.put_bundle(self.CACHE_TYPE_KEY, self.obj_id, bundle)
        except PolicyError as e:
            self.backend.set_status(self.obj_type, self.obj_id, "failed")
            self.backend.set_progress(self.obj_type, self.obj_id, str(e))
        except Exception:
            self.backend.set_status(self.obj_type, self.obj_id, "failed")
            self.backend.set_progress(
                self.obj_type,
                self.obj_id,
                "Internal Server Error. This incident will be reported.",
            )
            logging.exception("Bundle cooking failed.")
        else:
            self.backend.set_status(self.obj_type, self.obj_id, "done")
            self.backend.set_progress(self.obj_type, self.obj_id, None)
        finally:
            self.backend.send_notif(self.obj_type, self.obj_id)
