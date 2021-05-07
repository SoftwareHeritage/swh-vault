# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
This cooker creates tarballs containing a bare .git directory,
that can be unpacked and cloned like any git repository.

It works in three steps:

1. Write objects one by one in :file:`.git/objects/`
2. Calls ``git repack`` to pack all these objects into git packfiles.
3. Creates a tarball of the resulting repository

To avoid downloading and writing the same objects twice,
it checks the existence of the object file in the temporary directory.
To avoid sending a syscall every time, it also uses ``functools.lru_cache``,
as a first layer of cache before checking the file's existence.
"""

import datetime
import functools
import os.path
import subprocess
import tarfile
import tempfile
from typing import Any, Callable, Dict, List
import zlib

from swh.core.utils import grouper
from swh.graph.client import GraphArgumentException
from swh.model import identifiers
from swh.model.hashutil import hash_to_bytehex, hash_to_hex
from swh.model.model import (
    Person,
    Revision,
    RevisionType,
    Sha1Git,
    TimestampWithTimezone,
)
from swh.storage.algos.revisions_walker import DFSRevisionsWalker
from swh.vault.cookers.base import BaseVaultCooker

REVISION_BATCH_SIZE = 10000


class GitBareCooker(BaseVaultCooker):
    use_fsck = True

    def cache_type_key(self) -> str:
        return self.obj_type

    def check_exists(self):
        obj_type = self.obj_type.split("_")[0]
        if obj_type == "revision":
            return not list(self.storage.revision_missing([self.obj_id]))
        elif obj_type == "directory":
            return not list(self.storage.directory_missing([self.obj_id]))
        else:
            raise NotImplementedError(f"GitBareCooker for {obj_type}")

    def obj_swhid(self) -> identifiers.CoreSWHID:
        obj_type = self.obj_type.split("_")[0]
        return identifiers.CoreSWHID(
            object_type=identifiers.ObjectType[obj_type.upper()], object_id=self.obj_id,
        )

    def prepare_bundle(self):
        with tempfile.TemporaryDirectory(prefix="swh-vault-gitbare-") as workdir:
            # Initialize a Git directory
            self.workdir = workdir
            self.gitdir = os.path.join(workdir, "clone.git")
            os.mkdir(self.gitdir)
            self.init_git()

            # Load and write all the objects to disk
            self.load_subgraph(self.obj_type.split("_")[0], self.obj_id)

            # Write the root object as a ref.
            # This must be done before repacking; git-repack ignores orphan objects.
            self.write_refs()

            self.repack()
            self.write_archive()

    def init_git(self) -> None:
        subprocess.run(["git", "-C", self.gitdir, "init", "--bare"], check=True)

        # Create all possible dirs ahead of time, so we don't have to check for
        # existence every time.
        for byte in range(256):
            os.mkdir(os.path.join(self.gitdir, "objects", f"{byte:02x}"))

    def repack(self) -> None:
        if self.use_fsck:
            subprocess.run(["git", "-C", self.gitdir, "fsck"], check=True)

        # Add objects we wrote in a pack
        subprocess.run(["git", "-C", self.gitdir, "repack"], check=True)

        # Remove their non-packed originals
        subprocess.run(["git", "-C", self.gitdir, "prune-packed"], check=True)

    def write_refs(self):
        obj_type = self.obj_type.split("_")[0]
        if obj_type == "directory":
            # We need a synthetic revision pointing to the directory
            author = Person.from_fullname(
                b"swh-vault, git-bare cooker <robot@softwareheritage.org>"
            )
            dt = datetime.datetime.now(tz=datetime.timezone.utc)
            dt = dt.replace(microsecond=0)  # not supported by git
            date = TimestampWithTimezone.from_datetime(dt)
            revision = Revision(
                author=author,
                committer=author,
                date=date,
                committer_date=date,
                message=b"Initial commit",
                type=RevisionType.GIT,
                directory=self.obj_id,
                synthetic=True,
            )
            self.write_revision_node(revision.to_dict())
            head = revision.id
        elif obj_type == "revision":
            head = self.obj_id
        else:
            assert False, obj_type

        with open(os.path.join(self.gitdir, "refs", "heads", "master"), "wb") as fd:
            fd.write(hash_to_bytehex(head))

    def write_archive(self):
        with tarfile.TarFile(mode="w", fileobj=self.fileobj) as tf:
            tf.add(self.gitdir, arcname=f"{self.obj_swhid()}.git", recursive=True)

    def _obj_path(self, obj_id: Sha1Git):
        obj_id_hex = hash_to_hex(obj_id)
        directory = obj_id_hex[0:2]
        filename = obj_id_hex[2:]
        return os.path.join(self.gitdir, "objects", directory, filename)

    def object_exists(self, obj_id: Sha1Git) -> bool:
        return os.path.exists(self._obj_path(obj_id))

    def write_object(self, obj_id: Sha1Git, obj: bytes) -> bool:
        """Writes a git object on disk.

        Returns whether it was already written."""
        # Git requires objects to be zlib-compressed; but repacking decompresses and
        # removes them, so we don't need to compress them too much.
        data = zlib.compress(obj, level=1)

        with open(self._obj_path(obj_id), "wb") as fd:
            fd.write(data)
        return True

    def load_subgraph(self, obj_type, obj_id) -> None:
        if obj_type == "revision":
            self.load_revision_subgraph(obj_id)
        elif obj_type == "directory":
            self.load_directory_subgraph(obj_id)
        else:
            raise NotImplementedError(f"GitBareCooker.load_subgraph({obj_type!r}, ...)")

    def load_revision_subgraph(self, obj_id: Sha1Git) -> None:
        """Fetches a revision and all its children, and writes them to disk"""
        loaded_from_graph = False

        if self.graph:
            # First, try to cook using swh-graph, as it is more efficient than
            # swh-storage for querying the history
            obj_swhid = identifiers.CoreSWHID(
                object_type=identifiers.ObjectType.REVISION, object_id=obj_id,
            )
            try:
                revision_ids = (
                    swhid.object_id
                    for swhid in map(
                        identifiers.CoreSWHID.from_string,
                        self.graph.visit_nodes(str(obj_swhid), edges="rev:rev"),
                    )
                )
                for revision_id_group in grouper(revision_ids, REVISION_BATCH_SIZE):
                    self.load_revisions_and_directory_subgraphs(revision_id_group)
            except GraphArgumentException:
                # Revision not found in the graph
                pass
            else:
                loaded_from_graph = True

        if not loaded_from_graph:
            # If swh-graph is not available, or the revision is not yet in
            # swh-graph, fall back to self.storage.revision_log.
            walker = DFSRevisionsWalker(self.storage, obj_id)
            for revision in walker:
                self.write_revision_node(revision)
                self.load_directory_subgraph(revision["directory"])

    def load_revisions_and_directory_subgraphs(self, obj_ids: List[Sha1Git]) -> None:
        """Given a list of revision ids, loads these revisions and their directories;
        but not their parent revisions."""
        revisions = self.storage.revision_get(obj_ids)
        for revision in revisions:
            self.write_revision_node(revision.to_dict())
            self.load_directory_subgraph(revision.directory)

    def write_revision_node(self, revision: Dict[str, Any]) -> bool:
        """Writes a revision object to disk"""
        git_object = identifiers.revision_git_object(revision)
        return self.write_object(revision["id"], git_object)

    @functools.lru_cache(10240)
    def load_directory_subgraph(self, obj_id: Sha1Git) -> None:
        """Fetches a directory and all its children, and writes them to disk"""
        if self.object_exists(obj_id):
            # Checks if the object is already written on disk.
            # This rarely happens thanks to @lru_cache()
            return
        directory = self.load_directory_node(obj_id)
        entry_loaders: Dict[str, Callable[[Sha1Git], None]] = {
            "file": self.load_content,
            "dir": self.load_directory_subgraph,
            "rev": self.load_revision_subgraph,
        }
        for entry in directory["entries"]:
            entry_loader = entry_loaders[entry["type"]]
            entry_loader(entry["target"])

    def load_directory_node(self, obj_id: Sha1Git) -> Dict[str, Any]:
        """Fetches a directory, writes it to disk (non-recursively), and returns it."""
        entries = list(self.storage.directory_ls(obj_id, recursive=False))
        directory = {"id": obj_id, "entries": entries}
        git_object = identifiers.directory_git_object(directory)
        self.write_object(obj_id, git_object)
        return directory

    @functools.lru_cache(10240)
    def load_content(self, obj_id: Sha1Git) -> None:
        if self.object_exists(obj_id):
            # Checks if the object is already written on disk.
            # This rarely happens thanks to @lru_cache()
            return

        # TODO: add support of filtered objects, somehow?
        # It's tricky, because, by definition, we can't write a git object with
        # the expected hash, so git-fsck *will* choke on it.
        content_sha1 = self.storage.content_find({"sha1_git": obj_id})[0].sha1
        content = self.storage.content_get_data(content_sha1)
        self.write_object(obj_id, f"blob {len(content)}\0".encode("ascii") + content)
