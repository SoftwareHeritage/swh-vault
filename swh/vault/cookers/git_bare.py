# Copyright (C) 2021-2024  The Software Heritage developers
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

The first step is the most complex. When swh-graph is available, this roughly does
the following:

1. Find all the revisions and releases in the induced subgraph, adds them to
   todo-lists
2. Grab a batch from (release/revision/directory/content) todo-lists, and load them.
   Add directory and content objects they reference to the todo-list
3. If any todo-list is not empty, goto 1

When swh-graph is not available, steps 1 and 2 are merged, because revisions need
to be loaded in order to compute the subgraph.
"""

import concurrent
import datetime
import enum
import glob
import logging
import os.path
import re
import subprocess
import tarfile
import tempfile
from typing import Any, Dict, Iterable, List, NoReturn, Optional, Set
import zlib

import sentry_sdk

from swh.core.api.classes import stream_results_optional
from swh.model import git_objects
from swh.model.hashutil import hash_to_bytehex, hash_to_hex
from swh.model.model import (
    Directory,
    DirectoryEntry,
    Person,
    Release,
    ReleaseTargetType,
    Revision,
    RevisionType,
    Sha1Git,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
    TimestampWithTimezone,
)
from swh.model.swhids import CoreSWHID, ObjectType
from swh.objstorage.interface import objid_from_dict
from swh.storage.algos.revisions_walker import DFSRevisionsWalker
from swh.storage.algos.snapshot import snapshot_get_all_branches
from swh.vault.cookers.base import BaseVaultCooker
from swh.vault.to_disk import HIDDEN_MESSAGE, SKIPPED_MESSAGE

RELEASE_BATCH_SIZE = 10000
REVISION_BATCH_SIZE = 10000
DIRECTORY_BATCH_SIZE = 10000
CONTENT_BATCH_SIZE = 100


logger = logging.getLogger(__name__)


class RootObjectType(enum.Enum):
    DIRECTORY = "directory"
    REVISION = "revision"
    RELEASE = "release"
    SNAPSHOT = "snapshot"


def assert_never(value: NoReturn, msg) -> NoReturn:
    """mypy makes sure this function is never called, through exhaustive checking
    of ``value`` in the parent function.

    See https://mypy.readthedocs.io/en/latest/literal_types.html#exhaustive-checks
    for details.
    """
    assert False, msg


class GitBareCooker(BaseVaultCooker):
    BUNDLE_TYPE = "git_bare"
    SUPPORTED_OBJECT_TYPES = {ObjectType[obj_type.name] for obj_type in RootObjectType}

    use_fsck = True

    obj_type: RootObjectType

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.obj_type = RootObjectType[self.swhid.object_type.name]
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.thread_pool_size
        )

    def check_exists(self) -> bool:
        """Returns whether the root object is present in the archive."""
        if self.obj_type is RootObjectType.REVISION:
            return not list(self.storage.revision_missing([self.obj_id]))
        elif self.obj_type is RootObjectType.RELEASE:
            return not list(self.storage.release_missing([self.obj_id]))
        elif self.obj_type is RootObjectType.DIRECTORY:
            return not list(self.storage.directory_missing([self.obj_id]))
        elif self.obj_type is RootObjectType.SNAPSHOT:
            return not list(self.storage.snapshot_missing([self.obj_id]))
        else:
            assert_never(self.obj_type, f"Unexpected root object type: {self.obj_type}")

    def _push(self, stack: List[Sha1Git], obj_ids: Iterable[Sha1Git]) -> None:
        """Adds all the given ``obj_ids`` to the given ``stack``, unless they are
        already in ``self._seen``, and adds them to ``self._seen``."""
        assert not isinstance(obj_ids, bytes)
        revision_ids = [id_ for id_ in obj_ids if id_ not in self._seen]
        self._seen.update(revision_ids)
        stack.extend(revision_ids)

    def _pop(self, stack: List[Sha1Git], n: int) -> List[Sha1Git]:
        """Removes ``n`` object from the ``stack`` and returns them."""
        obj_ids = stack[-n:]
        stack[-n:] = []
        return obj_ids

    def prepare_bundle(self) -> None:
        """Main entry point. Initializes the state, creates the bundle, and
        sends it to the backend."""
        # Objects we will visit soon (aka. "todo-lists"):
        self._rel_stack: List[Sha1Git] = []
        self._rev_stack: List[Sha1Git] = []
        self._dir_stack: List[Sha1Git] = []
        self._cnt_stack: List[Sha1Git] = []

        # Set of objects already in any of the stacks:
        self._seen: Set[Sha1Git] = set()
        self._walker_state: Optional[Any] = None

        # Set of errors we expect git-fsck to raise at the end:
        self._expected_fsck_errors: Set[str] = set()

        with tempfile.TemporaryDirectory(prefix="swh-vault-gitbare-") as workdir:
            # Initialize a Git directory
            self.workdir = workdir
            self.gitdir = os.path.join(workdir, "clone.git")
            os.mkdir(self.gitdir)
            self.init_git()

            self.nb_loaded = 0

            # Add the root object to the stack of objects to visit
            self.push_subgraph(self.obj_type, self.obj_id)

            # Load and write all the objects to disk
            self.load_objects()

            self.backend.set_progress(
                self.BUNDLE_TYPE, self.swhid, "Writing references..."
            )

            # Write the root object as a ref (this step is skipped if it's a snapshot)
            # This must be done before repacking; git-repack ignores orphan objects.
            self.write_refs()

            self.backend.set_progress(
                self.BUNDLE_TYPE, self.swhid, "Checking content integrity"
            )

            if self.use_fsck:
                self.git_fsck()

            self.backend.set_progress(
                self.BUNDLE_TYPE, self.swhid, "Creating final bundle"
            )

            self.repack()

            self.write_archive()

            self.backend.set_progress(self.BUNDLE_TYPE, self.swhid, "Uploading bundle")

    def init_git(self) -> None:
        """Creates an empty :file:`.git` directory."""
        subprocess.run(["git", "-C", self.gitdir, "init", "--bare"], check=True)
        self.create_object_dirs()

        # Remove example hooks; they take ~40KB and we don't use them
        for filename in glob.glob(os.path.join(self.gitdir, "hooks", "*.sample")):
            os.unlink(filename)

    def create_object_dirs(self) -> None:
        """Creates all possible subdirectories of :file:`.git/objects/`"""
        # Create all possible dirs ahead of time, so we don't have to check for
        # existence every time.
        for byte in range(256):
            try:
                os.mkdir(os.path.join(self.gitdir, "objects", f"{byte:02x}"))
            except FileExistsError:
                pass

    def repack(self) -> None:
        """Moves all objects from :file:`.git/objects/` to a packfile."""
        try:
            subprocess.run(["git", "-C", self.gitdir, "repack", "-d"], check=True)
        except subprocess.CalledProcessError:
            logging.exception("git-repack failed with:")
            sentry_sdk.capture_exception()

        # Remove their non-packed originals
        subprocess.run(["git", "-C", self.gitdir, "prune-packed"], check=True)

    def git_fsck(self) -> None:
        """Runs git-fsck and ignores expected errors (eg. because of missing
        objects)."""
        proc = subprocess.run(
            ["git", "-C", self.gitdir, "fsck"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env={"LANG": "C.utf8"},
        )

        # Split on newlines not followed by a space
        errors = re.split("\n(?! )", proc.stdout.decode())

        errors = [
            error for error in errors if error and not error.startswith("warning ")
        ]

        unexpected_errors = set(errors) - self._expected_fsck_errors
        if unexpected_errors:
            logging.error(
                "Unexpected errors from git-fsck after cooking %s: %s",
                self.swhid,
                "\n".join(sorted(unexpected_errors)),
            )

    def _make_stub_directory_revision(self, dir_id: Sha1Git) -> Sha1Git:
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
        self.write_revision_node(revision)

        return revision.id

    def write_refs(self, snapshot=None) -> None:
        """Writes all files in :file:`.git/refs/`.

        For non-snapshot objects, this is only ``master``."""
        refs: Dict[bytes, bytes]  # ref name -> target
        if self.obj_type is RootObjectType.DIRECTORY:
            # We need a synthetic revision pointing to the directory
            rev_id = self._make_stub_directory_revision(self.obj_id)

            refs = {b"refs/heads/master": hash_to_bytehex(rev_id)}
        elif self.obj_type is RootObjectType.REVISION:
            refs = {b"refs/heads/master": hash_to_bytehex(self.obj_id)}
        elif self.obj_type is RootObjectType.RELEASE:
            (release,) = self.storage.release_get([self.obj_id])
            assert release, self.obj_id

            if release.name and re.match(rb"^[a-zA-Z0-9_.-]+$", release.name):
                release_name = release.name
            else:
                release_name = b"release"

            refs = {
                b"refs/tags/" + release_name: hash_to_bytehex(self.obj_id),
            }

            if release.target_type.value == ReleaseTargetType.REVISION.value:
                # Not necessary, but makes it easier to browse
                refs[b"refs/heads/master"] = hash_to_bytehex(release.target)
            # TODO: synthesize a master branch for other target types

        elif self.obj_type is RootObjectType.SNAPSHOT:
            if snapshot is None:
                # refs were already written in a previous step
                return
            branches = []
            for branch_name, branch in snapshot.branches.items():
                if branch is None:
                    logging.error(
                        "%s has dangling branch: %r", snapshot.swhid(), branch_name
                    )
                else:
                    branches.append((branch_name, branch))
            refs = {
                branch_name: (
                    b"ref: " + branch.target
                    if branch.target_type == SnapshotTargetType.ALIAS
                    else hash_to_bytehex(branch.target)
                )
                for (branch_name, branch) in branches
            }
        else:
            assert_never(self.obj_type, f"Unexpected root object type: {self.obj_type}")

        for ref_name, ref_target in refs.items():
            path = os.path.join(self.gitdir.encode(), ref_name)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as fd:
                fd.write(ref_target)

    def write_archive(self):
        """Creates the final .tar file."""
        with tarfile.TarFile(mode="w", fileobj=self.fileobj) as tf:
            tf.add(self.gitdir, arcname=f"{self.swhid}.git", recursive=True)

    def _obj_path(self, obj_id: Sha1Git):
        """Returns the absolute path of file (in :file:`.git/objects/`) that will
        contain the git object identified by the ``obj_id``."""
        return os.path.join(self.gitdir, self._obj_relative_path(obj_id))

    def _obj_relative_path(self, obj_id: Sha1Git):
        """Same as :meth:`_obj_path`, but relative."""
        obj_id_hex = hash_to_hex(obj_id)
        directory = obj_id_hex[0:2]
        filename = obj_id_hex[2:]
        return os.path.join("objects", directory, filename)

    def object_exists(self, obj_id: Sha1Git) -> bool:
        """Returns whether the object identified by the given ``obj_id`` was already
        written to a file in :file:`.git/object/`.

        This function ignores objects contained in a git pack."""
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

    def push_subgraph(self, obj_type: RootObjectType, obj_id) -> None:
        """Adds graph induced by the given ``obj_id`` without recursing through
        directories, to the todo-lists.

        If swh-graph is not available, this immediately loads revisions, as they
        need to be fetched in order to compute the subgraph, and fetching them
        immediately avoids duplicate fetches."""
        if self.obj_type is RootObjectType.REVISION:
            self.push_revision_subgraph(obj_id)
        elif self.obj_type is RootObjectType.DIRECTORY:
            self._push(self._dir_stack, [obj_id])
        elif self.obj_type is RootObjectType.SNAPSHOT:
            self.push_snapshot_subgraph(obj_id)
        elif self.obj_type is RootObjectType.RELEASE:
            self.push_releases_subgraphs([obj_id])
        else:
            assert_never(self.obj_type, f"Unexpected root object type: {self.obj_type}")

    def load_objects(self) -> None:
        """Repeatedly loads objects in the todo-lists, until all lists are empty."""

        futures = []
        while self._rel_stack or self._rev_stack or self._dir_stack or self._cnt_stack:
            nb_remaining = (
                len(self._rel_stack)
                + len(self._rev_stack)
                + len(self._dir_stack)
                + len(self._cnt_stack)
            )
            # We assume assume nb_remaining is a lower bound.
            # When the snapshot was loaded with swh-graph, this should be the exact
            # value, though.
            self.backend.set_progress(
                self.BUNDLE_TYPE,
                self.swhid,
                f"Processing... {self.nb_loaded} objects processed\n"
                f"Over {nb_remaining} remaining",
            )

            release_ids = self._pop(self._rel_stack, RELEASE_BATCH_SIZE)
            if release_ids:
                self.load_releases(release_ids)
                self.nb_loaded += len(release_ids)

            revision_ids = self._pop(self._rev_stack, REVISION_BATCH_SIZE)
            if revision_ids:
                self.load_revisions(revision_ids)
                self.nb_loaded += len(revision_ids)

            directory_ids = self._pop(self._dir_stack, DIRECTORY_BATCH_SIZE)
            if directory_ids:
                self.load_directories(directory_ids)
                self.nb_loaded += len(directory_ids)

            content_ids = self._pop(self._cnt_stack, CONTENT_BATCH_SIZE)
            if content_ids:
                futures += [
                    self.executor.submit(self.load_content, content_id)
                    for content_id in content_ids
                ]
                self.nb_loaded += len(content_ids)

        self.backend.set_progress(
            self.BUNDLE_TYPE,
            self.swhid,
            "Fetching contents bytes ...",
        )
        concurrent.futures.wait(futures)

    def push_revision_subgraph(self, obj_id: Sha1Git) -> None:
        """Fetches the graph of revisions induced by the given ``obj_id`` and adds
        them to ``self._rev_stack``.

        If swh-graph is not available, this requires fetching the revisions themselves,
        so they are directly loaded instead."""
        loaded_from_graph = False

        if self.graph:
            from swh.graph.http_client import GraphArgumentException

            # First, try to cook using swh-graph, as it is more efficient than
            # swh-storage for querying the history
            obj_swhid = CoreSWHID(
                object_type=ObjectType.REVISION,
                object_id=obj_id,
            )
            try:
                revision_ids = (
                    swhid.object_id
                    for swhid in map(
                        CoreSWHID.from_string,
                        self.graph.visit_nodes(str(obj_swhid), edges="rev:rev"),
                    )
                )
                self._push(self._rev_stack, revision_ids)
            except GraphArgumentException as e:
                logger.info(
                    "Revision %s not found in swh-graph, falling back to fetching "
                    "history using swh-storage. %s",
                    hash_to_hex(obj_id),
                    e.args[0],
                )
            else:
                loaded_from_graph = True

        if not loaded_from_graph:
            # If swh-graph is not available, or the revision is not yet in
            # swh-graph, fall back to self.storage.revision_log.
            # self.storage.revision_log also gives us the full revisions,
            # so we load them right now instead of just pushing them on the stack.
            walker = DFSRevisionsWalker(self.storage, obj_id, state=self._walker_state)
            for rev_d in walker:
                if isinstance(rev_d, Revision):
                    # TODO: Remove this conditional after swh-storage v3.0.0 is released
                    revision = rev_d
                else:
                    revision = Revision.from_dict(rev_d)
                self.write_revision_node(revision)
                self.nb_loaded += 1
                self._push(self._dir_stack, [revision.directory])
            # Save the state, so the next call to the walker won't return the same
            # revisions
            self._walker_state = walker.export_state()

    def push_snapshot_subgraph(self, obj_id: Sha1Git) -> None:
        """Fetches a snapshot and all its children, excluding directories and contents,
        and pushes them to the todo-lists.

        Also loads revisions if swh-graph is not available, see
        :meth:`push_revision_subgraph`."""
        loaded_from_graph = False

        if self.graph:
            revision_ids = []
            release_ids = []
            directory_ids = []
            content_ids = []

            from swh.graph.http_client import GraphArgumentException

            # First, try to cook using swh-graph, as it is more efficient than
            # swh-storage for querying the history
            obj_swhid = CoreSWHID(
                object_type=ObjectType.SNAPSHOT,
                object_id=obj_id,
            )
            try:
                swhids: Iterable[CoreSWHID] = map(
                    CoreSWHID.from_string,
                    self.graph.visit_nodes(str(obj_swhid), edges="snp:*,rel:*,rev:rev"),
                )
                for swhid in swhids:
                    if swhid.object_type is ObjectType.REVISION:
                        revision_ids.append(swhid.object_id)
                    elif swhid.object_type is ObjectType.RELEASE:
                        release_ids.append(swhid.object_id)
                    elif swhid.object_type is ObjectType.DIRECTORY:
                        directory_ids.append(swhid.object_id)
                    elif swhid.object_type is ObjectType.CONTENT:
                        content_ids.append(swhid.object_id)
                    elif swhid.object_type is ObjectType.SNAPSHOT:
                        assert (
                            swhid.object_id == obj_id
                        ), f"Snapshot {obj_id.hex()} references a different snapshot"
                    else:
                        assert_never(
                            swhid.object_type, f"Unexpected SWHID object type: {swhid}"
                        )
            except GraphArgumentException as e:
                logger.info(
                    "Snapshot %s not found in swh-graph, falling back to fetching "
                    "history for each branch. %s",
                    hash_to_hex(obj_id),
                    e.args[0],
                )
            else:
                self._push(self._rev_stack, revision_ids)
                self._push(self._rel_stack, release_ids)
                self._push(self._dir_stack, directory_ids)
                self._push(self._cnt_stack, content_ids)
                loaded_from_graph = True

        # TODO: when self.graph is available and supports edge labels, use it
        # directly to get branch names.
        snapshot: Optional[Snapshot] = snapshot_get_all_branches(self.storage, obj_id)
        assert snapshot, "Unknown snapshot"  # should have been caught by check_exists()
        for branch in snapshot.branches.values():
            if not loaded_from_graph:
                if branch is None:
                    logging.warning("Dangling branch: %r", branch)
                    continue
                assert isinstance(branch, SnapshotBranch)  # for mypy
                if branch.target_type is SnapshotTargetType.REVISION:
                    self.push_revision_subgraph(branch.target)
                elif branch.target_type is SnapshotTargetType.RELEASE:
                    self.push_releases_subgraphs([branch.target])
                elif branch.target_type is SnapshotTargetType.ALIAS:
                    # Nothing to do, this for loop also iterates on the target branch
                    # (if it exists)
                    pass
                elif branch.target_type is SnapshotTargetType.DIRECTORY:
                    self._push(self._dir_stack, [branch.target])
                elif branch.target_type is SnapshotTargetType.CONTENT:
                    self._push(self._cnt_stack, [branch.target])
                elif branch.target_type is SnapshotTargetType.SNAPSHOT:
                    if swhid.object_id != obj_id:
                        raise NotImplementedError(
                            f"{swhid} has a snapshot as a branch."
                        )
                else:
                    assert_never(
                        branch.target_type, f"Unexpected target type: {self.obj_type}"
                    )

        self.write_refs(snapshot=snapshot)

    def load_revisions(self, obj_ids: List[Sha1Git]) -> None:
        """Given a list of revision ids, loads these revisions and their directories;
        but not their parent revisions (ie. this is not recursive)."""
        ret: List[Optional[Revision]] = self.storage.revision_get(obj_ids)

        revisions: List[Revision] = list(filter(None, ret))
        if len(ret) != len(revisions):
            logger.error("Missing revision(s), ignoring them.")

        for revision in revisions:
            self.write_revision_node(revision)
        self._push(self._dir_stack, (rev.directory for rev in revisions))

    def write_revision_node(self, revision: Revision) -> bool:
        """Writes a revision object to disk"""
        git_object = revision.raw_manifest or git_objects.revision_git_object(revision)
        return self.write_object(revision.id, git_object)

    def load_releases(self, obj_ids: List[Sha1Git]) -> List[Release]:
        """Loads release objects, and returns them."""
        ret = self.storage.release_get(obj_ids)

        releases = list(filter(None, ret))
        if len(ret) != len(releases):
            logger.error("Missing release(s), ignoring them.")

        for release in releases:
            self.write_release_node(release)

        return releases

    def push_releases_subgraphs(self, obj_ids: List[Sha1Git]) -> None:
        """Given a list of release ids, loads these releases and adds their
        target to the list of objects to visit"""
        for release in self.load_releases(obj_ids):
            self.nb_loaded += 1
            assert release.target, "{release.swhid(}) has no target"
            if release.target_type is ReleaseTargetType.REVISION:
                self.push_revision_subgraph(release.target)
            elif release.target_type is ReleaseTargetType.DIRECTORY:
                self._push(self._dir_stack, [release.target])
            elif release.target_type is ReleaseTargetType.CONTENT:
                self._push(self._cnt_stack, [release.target])
            elif release.target_type is ReleaseTargetType.RELEASE:
                self.push_releases_subgraphs([release.target])
            elif release.target_type is ReleaseTargetType.SNAPSHOT:
                raise NotImplementedError(
                    f"{release.swhid()} targets a snapshot: {release.target!r}"
                )
            else:
                assert_never(
                    release.target_type,
                    f"Unexpected release target type: {release.target_type}",
                )

    def write_release_node(self, release: Release) -> bool:
        """Writes a release object to disk"""
        git_object = release.raw_manifest or git_objects.release_git_object(release)
        return self.write_object(release.id, git_object)

    def load_directories(self, obj_ids: List[Sha1Git]) -> None:
        if not obj_ids:
            return

        raw_manifests = self.storage.directory_get_raw_manifest(obj_ids)

        concurrent.futures.wait(
            self.executor.submit(self.load_directory, obj_id, raw_manifests.get(obj_id))
            for obj_id in obj_ids
        )

    def load_directory(self, obj_id: Sha1Git, raw_manifest: Optional[bytes]) -> None:
        # Load the directory
        entries_it: Optional[Iterable[DirectoryEntry]] = stream_results_optional(
            self.storage.directory_get_entries, obj_id
        )

        if entries_it is None:
            logger.error("Missing swh:1:dir:%s, ignoring.", hash_to_hex(obj_id))
            return

        directory = Directory(
            id=obj_id, entries=tuple(entries_it), raw_manifest=raw_manifest
        )
        git_object = raw_manifest or git_objects.directory_git_object(directory)
        self.write_object(obj_id, git_object)

        # Add children to the stack
        entry_loaders: Dict[str, Optional[List[Sha1Git]]] = {
            "file": self._cnt_stack,
            "dir": self._dir_stack,
            "rev": None,  # Do not include submodule targets (rejected by git-fsck)
        }
        for entry in directory.entries:
            stack = entry_loaders[entry.type]
            if stack is not None:
                self._push(stack, [entry.target])

    def load_content(self, obj_id: Sha1Git) -> None:
        # TODO: add support of filtered objects, somehow?
        # It's tricky, because, by definition, we can't write a git object with
        # the expected hash, so git-fsck *will* choke on it.
        content = self.storage.content_get([obj_id], "sha1_git")[0]

        if content is None:
            # FIXME: this may also happen for missing content
            self.write_content(obj_id, SKIPPED_MESSAGE)
            self._expect_mismatched_object_error(obj_id)
        elif content.status == "visible":
            hashes = objid_from_dict(content.hashes())
            if self.objstorage is None:
                datum = self.storage.content_get_data(hashes)
            else:
                datum = self.objstorage.get(hashes)

            if datum is None:
                logger.error(
                    "%s is visible, but is missing data. Skipping.", content.swhid()
                )
            else:
                self.write_content(content.sha1_git, datum)
        elif content.status == "hidden":
            self.write_content(obj_id, HIDDEN_MESSAGE)
            self._expect_mismatched_object_error(obj_id)
        elif content.status == "absent":
            assert False, f"content_get returned absent content {content.swhid()}"
        else:
            # TODO: When content.status will have type Literal, replace this with
            # assert_never
            assert False, f"{content.swhid} has status: {content.status!r}"

    def write_content(self, obj_id: Sha1Git, content: bytes) -> None:
        header = git_objects.git_object_header("blob", len(content))
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
