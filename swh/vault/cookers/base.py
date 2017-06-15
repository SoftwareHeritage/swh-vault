# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import abc
import io
import itertools
import logging
import os
import tarfile
import tempfile

from pathlib import Path

from swh.model import hashutil
from swh.storage import get_storage


def get_tar_bytes(path, arcname=None):
    path = Path(path)
    if not arcname:
        arcname = path.name
    tar_buffer = io.BytesIO()
    tar = tarfile.open(fileobj=tar_buffer, mode='w')
    tar.add(str(path), arcname=arcname)
    return tar_buffer.getbuffer()


SKIPPED_MESSAGE = (b'This content have not been retrieved in '
                   b'Software Heritage archive due to its size')


HIDDEN_MESSAGE = (b'This content is hidden')


class BaseVaultCooker(metaclass=abc.ABCMeta):
    """Abstract base class for the vault's bundle creators

    This class describes a common API for the cookers.

    To define a new cooker, inherit from this class and override:
    - CACHE_TYPE_KEY: key to use for the bundle to reference in cache
    - def cook(): cook the object into a bundle
    """
    CACHE_TYPE_KEY = None

    def __init__(self, config, obj_type, obj_id):
        """Initialize the cooker.

        The type of the object represented by the id depends on the
        concrete class. Very likely, each type of bundle will have its
        own cooker class.

        Args:
            storage: the storage object
            cache: the cache where to store the bundle
            obj_id: id of the object to be cooked into a bundle.
        """
        # Imported here to avoid circular dependency
        from swh.vault.backend import VaultBackend

        self.storage = get_storage(**config['storage'])
        self.backend = VaultBackend(config)
        self.obj_type = obj_type
        self.obj_id = hashutil.hash_to_bytes(obj_id)

    @abc.abstractmethod
    def prepare_bundle(self):
        """Implementation of the cooker. Yields chunks of the bundle bytes.

        Override this with the cooker implementation.
        """
        raise NotImplemented

    def cook(self):
        """Cook the requested object into a bundle
        """
        self.backend.set_status(self.obj_type, self.obj_id, 'pending')
        self.backend.set_progress(self.obj_type, self.obj_id, 'Initializing.')
        content_iter = self.prepare_bundle()

        self.update_cache(content_iter)
        self.backend.set_status(self.obj_type, self.obj_id, 'done')
        self.backend.set_progress(self.obj_type, self.obj_id, 'Done.')

        self.notify_bundle_ready()

    def update_cache(self, content_iter):
        """Update the cache with id and bundle_content.

        """
        self.backend.cache.add_stream(self.CACHE_TYPE_KEY,
                                      self.obj_id, content_iter)

    def notify_bundle_ready(self):
        self.backend.send_all_notifications(self.obj_type, self.obj_id)


class DirectoryBuilder:
    """Creates a cooked directory from its sha1_git in the db.

    Warning: This is NOT a directly accessible cooker, but a low-level
    one that executes the manipulations.

    """
    def __init__(self, storage):
        self.storage = storage

    def get_directory_bytes(self, dir_id):
        # Create temporary folder to retrieve the files into.
        root = bytes(tempfile.mkdtemp(prefix='directory.',
                                      suffix='.cook'), 'utf8')
        self.build_directory(dir_id, root)
        # Use the created directory to make a bundle with the data as
        # a compressed directory.
        bundle_content = self._create_bundle_content(
            root,
            hashutil.hash_to_hex(dir_id))
        return bundle_content

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
            status = file_data['status']
            perms = file_data['perms']
            if status == 'absent':
                self._create_file_absent(path)
            elif status == 'hidden':
                self._create_file_hidden(path)
            else:
                content = self._get_file_content(file_data['sha1'])
                self._create_file(path, content, perms)

    def _create_file(self, path, content, perms=0o100644):
        """Create the given file and fill it with content.

        """
        if perms not in (0o100644, 0o100755, 0o120000):
            logging.warning('File {} has invalid permission {}, '
                            'defaulting to 644.'.format(path, perms))
            perms = 0o100644

        if perms == 0o120000:  # Symbolic link
            os.symlink(content, path)
        else:
            with open(path, 'wb') as f:
                f.write(content)
            os.chmod(path, perms & 0o777)

    def _get_file_content(self, obj_id):
        """Get the content of the given file.

        """
        content = list(self.storage.content_get([obj_id]))[0]['data']
        return content

    def _create_file_absent(self, path):
        """Create a file that indicates a skipped content

        Create the given file but fill it with a specific content to
        indicate that the content have not been retrieved by the
        software heritage archive due to its size.

        """
        self._create_file(self, SKIPPED_MESSAGE)

    def _create_file_hidden(self, path):
        """Create a file that indicates an hidden content

        Create the given file but fill it with a specific content to
        indicate that the content could not be retrieved due to
        privacy policy.

        """
        self._create_file(self, HIDDEN_MESSAGE)

    def _create_bundle_content(self, path, hex_dir_id):
        """Create a bundle from the given directory

        Args:
            path: location of the directory to package.
            hex_dir_id: hex representation of the directory id

        Returns:
            bytes that represent the compressed directory as a bundle.

        """
        return get_tar_bytes(path.decode(), hex_dir_id)
