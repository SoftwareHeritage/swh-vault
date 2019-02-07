# Copyright (C) 2016-2018 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import collections
import os

from swh.model import hashutil
from swh.model.from_disk import mode_to_perms, DentryPerms
from swh.storage.algos.dir_iterators import dir_iterator

SKIPPED_MESSAGE = (b'This content has not been retrieved in the '
                   b'Software Heritage archive due to its size.')

HIDDEN_MESSAGE = (b'This content is hidden.')


def get_filtered_files_content(storage, files_data):
    """Retrieve the files specified by files_data and apply filters for skipped
    and missing contents.

    Args:
        storage: the storage from which to retrieve the objects
        files_data: list of file entries as returned by directory_ls()

    Yields:
        The entries given in files_data with a new 'content' key that points to
        the file content in bytes.

        The contents can be replaced by a specific message to indicate that
        they could not be retrieved (either due to privacy policy or because
        their sizes were too big for us to archive it).
    """
    contents_to_fetch = [f['sha1'] for f in files_data
                         if f['status'] == 'visible']
    contents_fetched = storage.content_get(contents_to_fetch)
    contents = {c['sha1']: c['data'] for c in contents_fetched}

    for file_data in files_data:
        if file_data['status'] == 'visible':
            content = contents[file_data['sha1']]
        elif file_data['status'] == 'absent':
            content = SKIPPED_MESSAGE
        elif file_data['status'] == 'hidden':
            content = HIDDEN_MESSAGE

        yield {'content': content, **file_data}


def apply_chunked(func, input_list, chunk_size):
    """Apply func on input_list divided in chunks of size chunk_size"""
    for i in range(0, len(input_list), chunk_size):
        yield from func(input_list[i:i + chunk_size])


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
        # Split into files, revisions and directory data.
        entries = collections.defaultdict(list)
        for entry in dir_iterator(self.storage, self.dir_id):
            entries[entry['type']].append(entry)

        # Recreate the directory's subtree and then the files into it.
        self._create_tree(entries['dir'])
        self._create_files(entries['file'])
        self._create_revisions(entries['rev'])

    def _create_tree(self, directories):
        """Create a directory tree from the given paths

        The tree is created from `root` and each given directory in
        `directories` will be created.
        """
        # Directories are sorted by depth so they are created in the
        # right order
        bsep = os.path.sep.encode()
        directories = sorted(directories,
                             key=lambda x: len(x['path'].split(bsep)))
        for dir in directories:
            os.makedirs(os.path.join(self.root, dir['path']))

    def _create_files(self, files_data):
        """Create the files in the tree and fetch their contents."""
        f = functools.partial(get_filtered_files_content, self.storage)
        files_data = apply_chunked(f, files_data, 1000)

        for file_data in files_data:
            path = os.path.join(self.root, file_data['path'])
            self._create_file(path, file_data['content'], file_data['perms'])

    def _create_revisions(self, revs_data):
        """Create the revisions in the tree as broken symlinks to the target
        identifier."""
        for file_data in revs_data:
            path = os.path.join(self.root, file_data['path'])
            self._create_file(path, hashutil.hash_to_hex(file_data['target']),
                              mode=0o120000)

    def _create_file(self, path, content, mode=0o100644):
        """Create the given file and fill it with content."""
        perms = mode_to_perms(mode)
        if perms == DentryPerms.symlink:
            os.symlink(content, path)
        else:
            with open(path, 'wb') as f:
                f.write(content)
            os.chmod(path, perms.value)
