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

It keeps a set of all written (or about-to-be-written) object hashes in memory
to avoid downloading and writing the same objects twice.
"""

import datetime
import os.path
import re
import subprocess
import tarfile
import tempfile
from typing import Any, Dict, Iterable, List, Set
import zlib

from swh.core.api.classes import stream_results
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
from swh.vault.to_disk import HIDDEN_MESSAGE, SKIPPED_MESSAGE

REVISION_BATCH_SIZE = 10000
DIRECTORY_BATCH_SIZE = 10000
CONTENT_BATCH_SIZE = 100


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

    def _push(self, stack: List[Sha1Git], obj_ids: Iterable[Sha1Git]) -> None:
        assert not isinstance(obj_ids, bytes)
        revision_ids = [id_ for id_ in obj_ids if id_ not in self._seen]
        self._seen.update(revision_ids)
        stack.extend(revision_ids)

    def _pop(self, stack: List[Sha1Git], n: int) -> List[Sha1Git]:
        obj_ids = stack[-n:]
        stack[-n:] = []
        return obj_ids

    def prepare_bundle(self):
        # Objects we will visit soon:
        self._rev_stack: List[Sha1Git] = []
        self._dir_stack: List[Sha1Git] = []
        self._cnt_stack: List[Sha1Git] = []

        # Set of objects already in any of the stacks:
        self._seen: Set[Sha1Git] = set()

        # Set of errors we expect git-fsck to raise at the end:
        self._expected_fsck_errors = set()

        with tempfile.TemporaryDirectory(prefix="swh-vault-gitbare-") as workdir:
            # Initialize a Git directory
            self.workdir = workdir
            self.gitdir = os.path.join(workdir, "clone.git")
            os.mkdir(self.gitdir)
            self.init_git()

            # Add the root object to the stack of objects to visit
            self.push_subgraph(self.obj_type.split("_")[0], self.obj_id)

            # Load and write all the objects to disk
            self.load_objects()

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
            self.git_fsck()

        # Add objects we wrote in a pack
        subprocess.run(["git", "-C", self.gitdir, "repack"], check=True)

        # Remove their non-packed originals
        subprocess.run(["git", "-C", self.gitdir, "prune-packed"], check=True)

    def git_fsck(self) -> None:
        proc = subprocess.run(
            ["git", "-C", self.gitdir, "fsck"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env={"LANG": "C.utf8"},
        )
        if not self._expected_fsck_errors:
            # All went well, there should not be any error
            proc.check_returncode()
            return

        # Split on newlines not followed by a space
        errors = re.split("\n(?! )", proc.stdout.decode())

        unexpected_errors = set(filter(bool, errors)) - self._expected_fsck_errors
        if unexpected_errors:
            raise Exception(
                "\n".join(
                    ["Unexpected errors from git-fsck:"] + sorted(unexpected_errors)
                )
            )

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
        return os.path.join(self.gitdir, self._obj_relative_path(obj_id))

    def _obj_relative_path(self, obj_id: Sha1Git):
        obj_id_hex = hash_to_hex(obj_id)
        directory = obj_id_hex[0:2]
        filename = obj_id_hex[2:]
        return os.path.join("objects", directory, filename)

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

    def push_subgraph(self, obj_type, obj_id) -> None:
        if obj_type == "revision":
            self.push_revision_subgraph(obj_id)
        elif obj_type == "directory":
            self._push(self._dir_stack, [obj_id])
        else:
            raise NotImplementedError(
                f"GitBareCooker.queue_subgraph({obj_type!r}, ...)"
            )

    def load_objects(self) -> None:
        while self._rev_stack or self._dir_stack or self._cnt_stack:
            revision_ids = self._pop(self._rev_stack, REVISION_BATCH_SIZE)
            self.load_revisions(revision_ids)

            directory_ids = self._pop(self._dir_stack, DIRECTORY_BATCH_SIZE)
            self.load_directories(directory_ids)

            content_ids = self._pop(self._cnt_stack, CONTENT_BATCH_SIZE)
            self.load_contents(content_ids)

    def push_revision_subgraph(self, obj_id: Sha1Git) -> None:
        """Fetches a revision and all its children, and writes them to disk"""
        loaded_from_graph = False

        if self.graph:
            from swh.graph.client import GraphArgumentException

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
                self._push(self._rev_stack, revision_ids)
            except GraphArgumentException:
                # Revision not found in the graph
                pass
            else:
                loaded_from_graph = True

        if not loaded_from_graph:
            # If swh-graph is not available, or the revision is not yet in
            # swh-graph, fall back to self.storage.revision_log.
            # self.storage.revision_log also gives us the full revisions,
            # so we load them right now instead of just pushing them on the stack.
            walker = DFSRevisionsWalker(self.storage, obj_id)
            for revision in walker:
                self.write_revision_node(revision)
                self._push(self._dir_stack, [revision["directory"]])

    def load_revisions(self, obj_ids: List[Sha1Git]) -> None:
        """Given a list of revision ids, loads these revisions and their directories;
        but not their parent revisions."""
        revisions = self.storage.revision_get(obj_ids)
        for revision in revisions:
            self.write_revision_node(revision.to_dict())
        self._push(self._dir_stack, (rev.directory for rev in revisions))

    def write_revision_node(self, revision: Dict[str, Any]) -> bool:
        """Writes a revision object to disk"""
        git_object = identifiers.revision_git_object(revision)
        return self.write_object(revision["id"], git_object)

    def load_directories(self, obj_ids: List[Sha1Git]) -> None:
        for obj_id in obj_ids:
            self.load_directory(obj_id)

    def load_directory(self, obj_id: Sha1Git) -> None:
        # Load the directory
        entries = [
            entry.to_dict()
            for entry in stream_results(self.storage.directory_get_entries, obj_id)
        ]
        directory = {"id": obj_id, "entries": entries}
        git_object = identifiers.directory_git_object(directory)
        self.write_object(obj_id, git_object)

        # Add children to the stack
        entry_loaders: Dict[str, List[Sha1Git]] = {
            "file": self._cnt_stack,
            "dir": self._dir_stack,
            "rev": self._rev_stack,
        }
        for entry in directory["entries"]:
            stack = entry_loaders[entry["type"]]
            self._push(stack, [entry["target"]])

    def load_contents(self, obj_ids: List[Sha1Git]) -> None:
        # TODO: add support of filtered objects, somehow?
        # It's tricky, because, by definition, we can't write a git object with
        # the expected hash, so git-fsck *will* choke on it.
        contents = self.storage.content_get(obj_ids, "sha1_git")

        visible_contents = []
        for (obj_id, content) in zip(obj_ids, contents):
            if content is None:
                # FIXME: this may also happen for missing content
                self.write_content(obj_id, SKIPPED_MESSAGE)
                self._expect_mismatched_object_error(obj_id)
            elif content.status == "visible":
                visible_contents.append(content)
            elif content.status == "hidden":
                self.write_content(obj_id, HIDDEN_MESSAGE)
                self._expect_mismatched_object_error(obj_id)
            else:
                assert False, (
                    f"unexpected status {content.status!r} "
                    f"for content {hash_to_hex(content.sha1_git)}"
                )

        if self.objstorage is None:
            for content in visible_contents:
                data = self.storage.content_get_data(content.sha1)
                self.write_content(content.sha1_git, data)
        else:
            content_data = self.objstorage.get_batch(c.sha1 for c in visible_contents)
            for (content, data) in zip(contents, content_data):
                self.write_content(content.sha1_git, data)

    def write_content(self, obj_id: Sha1Git, content: bytes) -> None:
        header = identifiers.git_object_header("blob", len(content))
        self.write_object(obj_id, header + content)

    def _expect_mismatched_object_error(self, obj_id):
        obj_id_hex = hash_to_hex(obj_id)
        obj_path = self._obj_relative_path(obj_id)

        # For Git < 2.21:
        self._expected_fsck_errors.add(
            f"error: sha1 mismatch for ./{obj_path} (expected {obj_id_hex})"
        )
        # For Git >= 2.21:
        self._expected_fsck_errors.add(
            f"error: hash mismatch for ./{obj_path} (expected {obj_id_hex})"
        )

        self._expected_fsck_errors.add(
            f"error: {obj_id_hex}: object corrupt or missing: ./{obj_path}"
        )
        self._expected_fsck_errors.add(f"missing blob {obj_id_hex}")
