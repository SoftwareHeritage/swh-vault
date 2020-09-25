# Copyright (C) 2016-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from pathlib import Path
import tarfile
import tempfile

from swh.model import hashutil
from swh.vault.cookers.base import BaseVaultCooker
from swh.vault.cookers.utils import revision_log
from swh.vault.to_disk import DirectoryBuilder


class RevisionFlatCooker(BaseVaultCooker):
    """Cooker to create a revision_flat bundle """

    CACHE_TYPE_KEY = "revision_flat"

    def check_exists(self):
        return not list(self.storage.revision_missing([self.obj_id]))

    def prepare_bundle(self):
        with tempfile.TemporaryDirectory(prefix="tmp-vault-revision-") as td:
            root = Path(td)
            for revision in revision_log(self.storage, self.obj_id):
                revdir = root / hashutil.hash_to_hex(revision["id"])
                revdir.mkdir()
                directory_builder = DirectoryBuilder(
                    self.storage, str(revdir).encode(), revision["directory"]
                )
                directory_builder.build()
            with tarfile.open(fileobj=self.fileobj, mode="w:gz") as tar:
                tar.add(td, arcname=hashutil.hash_to_hex(self.obj_id))
