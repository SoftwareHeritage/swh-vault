# Copyright (C) 2016-2018 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import itertools
import os

from swh.model.from_disk import mode_to_perms, DentryPerms

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
    """Reconstructs the on-disk representation of a directory in the storage.
    """

    def __init__(self, storage, root, dir_id):
        """Initialize the directory builder.

        Args:
            storage: the storage object
            root: the path where the directory should be reconstructed
            dir_id: the identifier of the directory in the storage
        """
        self.storage = storage
        self.root = root
        self.dir_id = dir_id

    def build(self):
        """Perform the reconstruction of the directory in the given root."""
        # Retrieve data from the database.
        data = self.storage.directory_ls(self.dir_id, recursive=True)

        # Split into files and directory data.
        # TODO(seirl): also handle revision data.
        data1, data2 = itertools.tee(data, 2)
        dir_data = (entry['name'] for entry in data1 if entry['type'] == 'dir')
        file_data = (entry for entry in data2 if entry['type'] == 'file')

        # Recreate the directory's subtree and then the files into it.
        self._create_tree(dir_data)
        self._create_files(file_data)

    def _create_tree(self, directory_paths):
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
            os.makedirs(os.path.join(self.root, dir_name))

    def _create_files(self, file_datas):
        """Create the files according to their status."""
        # Then create the files
        for file_data in file_datas:
            path = os.path.join(self.root, file_data['name'])
            content = get_filtered_file_content(self.storage, file_data)
            self._create_file(path, content, file_data['perms'])

    def _create_file(self, path, content, mode=0o100644):
        """Create the given file and fill it with content."""
        perms = mode_to_perms(mode)
        if perms == DentryPerms.symlink:
            os.symlink(content, path)
        else:
            with open(path, 'wb') as f:
                f.write(content)
            os.chmod(path, perms.value)

    def _get_file_content(self, obj_id):
        """Get the content of the given file."""
        content = list(self.storage.content_get([obj_id]))[0]['data']
        return content
