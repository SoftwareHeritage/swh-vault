# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import io
import itertools
import logging
import os

from swh.core import config
from swh.model import hashutil
from swh.model.from_disk import mode_to_perms, DentryPerms
from swh.storage import get_storage
from swh.vault.api.client import RemoteVaultClient


DEFAULT_CONFIG_PATH = 'vault/cooker'
DEFAULT_CONFIG = {
    'storage': ('dict', {
        'cls': 'remote',
        'args': {
            'url': 'http://localhost:5002/',
        },
    }),
    'vault_url': ('str', 'http://localhost:5005/'),
    'max_bundle_size': ('int', 2 ** 29),  # 512 MiB
}


class PolicyError(Exception):
    """Raised when the bundle violates the cooking policy."""
    pass


class BundleTooLargeError(PolicyError):
    """Raised when the bundle is too large to be cooked."""
    pass


class BytesIOBundleSizeLimit(io.BytesIO):
    def __init__(self, *args, size_limit=None, **kwargs):
        super().__init__(self, *args, **kwargs)
        self.size_limit = size_limit

    def write(self, chunk):
        if ((self.size_limit is not None
             and self.getbuffer().nbytes + len(chunk) > self.size_limit)):
            raise BundleTooLargeError(
                "The requested bundle exceeds the maximum allowed "
                "size of {} bytes.".format(self.size_limit))
        return super().write(chunk)


class BaseVaultCooker(metaclass=abc.ABCMeta):
    """Abstract base class for the vault's bundle creators

    This class describes a common API for the cookers.

    To define a new cooker, inherit from this class and override:
    - CACHE_TYPE_KEY: key to use for the bundle to reference in cache
    - def cook(): cook the object into a bundle
    """
    CACHE_TYPE_KEY = None

    def __init__(self, obj_type, obj_id):
        """Initialize the cooker.

        The type of the object represented by the id depends on the
        concrete class. Very likely, each type of bundle will have its
        own cooker class.

        Args:
            storage: the storage object
            cache: the cache where to store the bundle
            obj_id: id of the object to be cooked into a bundle.
        """
        self.config = config.load_named_config(DEFAULT_CONFIG_PATH,
                                               DEFAULT_CONFIG)
        self.obj_type = obj_type
        self.obj_id = hashutil.hash_to_bytes(obj_id)
        self.backend = RemoteVaultClient(self.config['vault_url'])
        self.storage = get_storage(**self.config['storage'])
        self.max_bundle_size = self.config['max_bundle_size']

    @abc.abstractmethod
    def check_exists(self):
        """Checks that the requested object exists and can be cooked.

        Override this in the cooker implementation.
        """
        raise NotImplemented

    @abc.abstractmethod
    def prepare_bundle(self):
        """Implementation of the cooker. Yields chunks of the bundle bytes.

        Override this with the cooker implementation.
        """
        raise NotImplemented

    def write(self, chunk):
        self.fileobj.write(chunk)

    def cook(self):
        """Cook the requested object into a bundle
        """
        self.backend.set_status(self.obj_type, self.obj_id, 'pending')
        self.backend.set_progress(self.obj_type, self.obj_id, 'Processing...')

        self.fileobj = BytesIOBundleSizeLimit(size_limit=self.max_bundle_size)
        try:
            self.prepare_bundle()
            bundle = self.fileobj.getvalue()
        except PolicyError as e:
            self.backend.set_status(self.obj_type, self.obj_id, 'failed')
            self.backend.set_progress(self.obj_type, self.obj_id, str(e))
        except Exception as e:
            self.backend.set_status(self.obj_type, self.obj_id, 'failed')
            self.backend.set_progress(
                self.obj_type, self.obj_id,
                "Internal Server Error. This incident will be reported.")
            logging.exception("Bundle cooking failed.")
        else:
            # TODO: use proper content streaming instead of put_bundle()
            self.backend.put_bundle(self.CACHE_TYPE_KEY, self.obj_id, bundle)
            self.backend.set_status(self.obj_type, self.obj_id, 'done')
            self.backend.set_progress(self.obj_type, self.obj_id, None)
        finally:
            self.backend.send_notif(self.obj_type, self.obj_id)


SKIPPED_MESSAGE = (b'This content has not been retrieved in the '
                   b'Software Heritage archive due to its size.')

HIDDEN_MESSAGE = (b'This content is hidden.')


def get_filtered_file_content(storage, file_data):
    """Retrieve the file specified by file_data and apply filters for skipped
    and missing contents.

    Args:
        storage: the storage from which to retrieve the object
        file_data: file entry descriptor as returned by directory_ls()

    Returns:
        Bytes containing the specified content. The content will be replaced by
        a specific message to indicate that the content could not be retrieved
        (either due to privacy policy or because its size was too big for us to
        archive it).
    """

    assert file_data['type'] == 'file'

    if file_data['status'] == 'absent':
        return SKIPPED_MESSAGE
    elif file_data['status'] == 'hidden':
        return HIDDEN_MESSAGE
    else:
        return list(storage.content_get([file_data['sha1']]))[0]['data']


class DirectoryBuilder:
    """Creates a cooked directory from its sha1_git in the db.

    Warning: This is NOT a directly accessible cooker, but a low-level
    one that executes the manipulations.

    """
    def __init__(self, storage):
        self.storage = storage

    def build_directory(self, dir_id, root):
        # Retrieve data from the database.
        data = self.storage.directory_ls(dir_id, recursive=True)

        # Split into files and directory data.
        # TODO(seirl): also handle revision data.
        data1, data2 = itertools.tee(data, 2)
        dir_data = (entry['name'] for entry in data1 if entry['type'] == 'dir')
        file_data = (entry for entry in data2 if entry['type'] == 'file')

        # Recreate the directory's subtree and then the files into it.
        self._create_tree(root, dir_data)
        self._create_files(root, file_data)

    def _create_tree(self, root, directory_paths):
        """Create a directory tree from the given paths

        The tree is created from `root` and each given path in
        `directory_paths` will be created.

        """
        # Directories are sorted by depth so they are created in the
        # right order
        bsep = bytes(os.path.sep, 'utf8')
        dir_names = sorted(
            directory_paths,
            key=lambda x: len(x.split(bsep)))
        for dir_name in dir_names:
            os.makedirs(os.path.join(root, dir_name))

    def _create_files(self, root, file_datas):
        """Create the files according to their status.

        """
        # Then create the files
        for file_data in file_datas:
            path = os.path.join(root, file_data['name'])
            content = get_filtered_file_content(self.storage, file_data)
            self._create_file(path, content, file_data['perms'])

    def _create_file(self, path, content, mode=0o100644):
        """Create the given file and fill it with content.

        """
        perms = mode_to_perms(mode)
        if perms == DentryPerms.symlink:
            os.symlink(content, path)
        else:
            with open(path, 'wb') as f:
                f.write(content)
            os.chmod(path, perms.value)

    def _get_file_content(self, obj_id):
        """Get the content of the given file.

        """
        content = list(self.storage.content_get([obj_id]))[0]['data']
        return content
