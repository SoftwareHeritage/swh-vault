# Copyright (C) 2016-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tarfile
import tempfile

from swh.model.swhids import ObjectType
from swh.vault.cookers.base import BaseVaultCooker, DirectoryTooLargeError
from swh.vault.to_disk import REFUSED_DIRECTORIES, DirectoryBuilder


class DirectoryCooker(BaseVaultCooker):
    """Cooker to create a directory bundle"""

    BUNDLE_TYPE = "flat"
    SUPPORTED_OBJECT_TYPES = {ObjectType.DIRECTORY}

    def check_exists(self):
        if list(self.storage.directory_missing([self.obj_id])):
            return False

        # Already known to be too large from a previous task
        if REFUSED_DIRECTORIES.refuses(
            self.obj_id, self.max_directory_entries, self.max_directory_size
        ):
            raise DirectoryTooLargeError(
                self.swhid, "more than one of the configured limits"
            )

        return True

    def prepare_bundle(self):
        with tempfile.TemporaryDirectory(prefix="tmp-vault-directory-") as td:
            directory_builder = DirectoryBuilder(
                storage=self.storage,
                root=td.encode(),
                dir_id=self.obj_id,
                thread_pool_size=self.thread_pool_size,
                objstorage=self.objstorage,
                max_directory_entries=self.max_directory_entries,
                max_directory_size=self.max_directory_size,
                max_cooking_time=self.max_cooking_time,
            )
            directory_builder.build()
            with tarfile.open(fileobj=self.fileobj, mode="w:gz") as tar:
                tar.add(td, arcname=str(self.swhid))
