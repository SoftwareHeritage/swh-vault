# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tarfile
import tempfile

from swh.vault.cookers.base import BaseVaultCooker, DirectoryBuilder
from swh.model import hashutil


class DirectoryCooker(BaseVaultCooker):
    """Cooker to create a directory bundle """
    CACHE_TYPE_KEY = 'directory'

    def check_exists(self):
        return not list(self.storage.directory_missing([self.obj_id]))

    def prepare_bundle(self):
        directory_builder = DirectoryBuilder(self.storage)
        with tempfile.TemporaryDirectory(prefix='tmp-vault-directory-') as td:
            directory_builder.build_directory(self.obj_id, td.encode())
            tar = tarfile.open(fileobj=self.fileobj, mode='w')
            tar.add(td, arcname=hashutil.hash_to_hex(self.obj_id))
