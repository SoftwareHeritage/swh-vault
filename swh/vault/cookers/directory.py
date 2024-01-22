# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tarfile
import tempfile

from swh.model.swhids import ObjectType
from swh.vault.cookers.base import BaseVaultCooker
from swh.vault.to_disk import DirectoryBuilder


class DirectoryCooker(BaseVaultCooker):
    """Cooker to create a directory bundle"""

    BUNDLE_TYPE = "flat"
    SUPPORTED_OBJECT_TYPES = {ObjectType.DIRECTORY}

    def check_exists(self):
        return not list(self.storage.directory_missing([self.obj_id]))

    def prepare_bundle(self):
        with tempfile.TemporaryDirectory(prefix="tmp-vault-directory-") as td:
            directory_builder = DirectoryBuilder(
                storage=self.storage,
                root=td.encode(),
                dir_id=self.obj_id,
                thread_pool_size=self.thread_pool_size,
                objstorage=self.objstorage,
            )
            directory_builder.build()
            with tarfile.open(fileobj=self.fileobj, mode="w:gz") as tar:
                tar.add(td, arcname=str(self.swhid))
