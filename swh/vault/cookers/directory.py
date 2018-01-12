# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.vault.cookers.base import BaseVaultCooker, DirectoryBuilder


class DirectoryCooker(BaseVaultCooker):
    """Cooker to create a directory bundle """
    CACHE_TYPE_KEY = 'directory'

    def check_exists(self):
        return not list(self.storage.directory_missing([self.obj_id]))

    def prepare_bundle(self):
        directory_builder = DirectoryBuilder(self.storage)
        directory_builder.write_directory_bytes(self.obj_id, self.fileobj)
