# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tarfile
import tempfile

from swh.model import hashutil
from swh.vault.cookers.base import BaseVaultCooker
from swh.vault.to_disk import DirectoryBuilder


class DirectoryCooker(BaseVaultCooker):
    """Cooker to create a directory bundle """
    CACHE_TYPE_KEY = 'directory'

    def check_exists(self):
        return not list(self.storage.directory_missing([self.obj_id]))

    def prepare_bundle(self):
        with tempfile.TemporaryDirectory(prefix='tmp-vault-directory-') as td:
            directory_builder = DirectoryBuilder(
                self.storage, td.encode(), self.obj_id)
            directory_builder.build()
            with tarfile.open(fileobj=self.fileobj, mode='w:gz') as tar:
                tar.add(td, arcname=hashutil.hash_to_hex(self.obj_id))
