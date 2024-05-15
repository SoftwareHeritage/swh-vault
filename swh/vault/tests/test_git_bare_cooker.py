# Copyright (C) 2021-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
This module contains additional tests for the bare cooker.
Generic cooker tests (eg. without swh-graph) in test_cookers.py also
run on the bare cooker.
"""

import datetime
import enum
from functools import partial
import io
import os
import subprocess
import tarfile
import tempfile
import unittest.mock

import attr
import pytest
from pytest import param
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.model.from_disk import DentryPerms
from swh.model.model import (
    Content,
    Directory,
    DirectoryEntry,
    ObjectType,
    Person,
    Release,
    Revision,
    RevisionType,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
    TimestampWithTimezone,
)
from swh.storage import get_storage
from swh.storage.postgresql.storage import Storage
from swh.vault.cookers.git_bare import GitBareCooker
from swh.vault.in_memory_backend import InMemoryVaultBackend

storage_postgresql_proc = factories.postgresql_proc(
    load=[partial(initialize_database_for_module, "storage", Storage.current_version)],
)

storage_postgresql = factories.postgresql("storage_postgresql_proc")


@pytest.fixture
def swh_storage(storage_postgresql):
    return get_storage(
        "postgresql", db=storage_postgresql.info.dsn, objstorage={"cls": "memory"}
    )


class RootObjects(enum.Enum):
    REVISION = enum.auto()
    SNAPSHOT = enum.auto()
    RELEASE = enum.auto()
    WEIRD_RELEASE = enum.auto()  # has a : in the name + points to another release


@pytest.mark.graph
@pytest.mark.parametrize(
    "root_object,up_to_date_graph,tag,weird_branches",
    [
        param(
            RootObjects.REVISION,
            False,
            False,
            False,
            id="rev, outdated graph, no tag/tree/blob",
        ),
        param(
            RootObjects.REVISION,
            True,
            False,
            False,
            id="rev, updated graph, no tag/tree/blob",
        ),
        param(
            RootObjects.RELEASE,
            False,
            False,
            False,
            id="rel, outdated graph, no tag/tree/blob",
        ),
        param(
            RootObjects.RELEASE,
            True,
            False,
            False,
            id="rel, updated graph, no tag/tree/blob",
        ),
        param(
            RootObjects.WEIRD_RELEASE,
            True,
            False,
            False,
            id="weird rel, updated graph, no tag/tree/blob",
        ),
        param(
            RootObjects.SNAPSHOT,
            False,
            False,
            False,
            id="snp, outdated graph, no tag/tree/blob",
        ),
        param(
            RootObjects.SNAPSHOT,
            True,
            False,
            False,
            id="snp, updated graph, no tag/tree/blob",
        ),
        param(
            RootObjects.SNAPSHOT,
            False,
            True,
            False,
            id="snp, outdated graph, w/ tag, no tree/blob",
        ),
        param(
            RootObjects.SNAPSHOT,
            True,
            True,
            False,
            id="snp, updated graph, w/ tag, no tree/blob",
        ),
        param(
            RootObjects.SNAPSHOT,
            False,
            True,
            True,
            id="snp, outdated graph, w/ tag, tree, and blob",
        ),
        param(
            RootObjects.SNAPSHOT,
            True,
            True,
            True,
            id="snp, updated graph, w/ tag, tree, and blob",
        ),
    ],
)
def test_graph_revisions(
    swh_storage, up_to_date_graph, root_object, tag, weird_branches
):
    r"""
    Build objects::

                                     snp
                                    /|||\
                                   / ||| \
                        rel2 <----°  /|\  \----> rel4
                         |          / | \         |
                         v         /  v  \        v
          rev1  <------ rev2 <----°  dir4 \      rel3
           |             |            |    \      |
           v             v            v     \     |
          dir1          dir2         dir3   |     |
           |           /   |          |     |     |
           v          /    v          v     v     v
          cnt1  <----°    cnt2       cnt3  cnt4  cnt5

    If up_to_date_graph is true, then swh-graph contains all objects.
    Else, cnt4, cnt5, dir4, rev2, rel2, rel3, and snp are missing from the graph.

    If tag is False, rel2 is excluded.

    If weird_branches is False, dir4, cnt4, rel3, rel4, and cnt5 are excluded.
    """
    from swh.graph.http_naive_client import NaiveClient as GraphClient

    # Create objects:

    date = TimestampWithTimezone.from_datetime(
        datetime.datetime(2021, 5, 7, 8, 43, 59, tzinfo=datetime.timezone.utc)
    )
    author = Person.from_fullname(b"Foo <foo@example.org>")
    cnt1 = Content.from_data(b"correct")
    cnt2 = Content.from_data(b"horse")
    cnt3 = Content.from_data(b"battery")
    cnt4 = Content.from_data(b"staple")
    cnt5 = Content.from_data(b"Tr0ub4dor&3")
    dir1 = Directory(
        entries=(
            DirectoryEntry(
                name=b"file1",
                type="file",
                perms=DentryPerms.content,
                target=cnt1.sha1_git,
            ),
        )
    )
    dir2 = Directory(
        entries=(
            DirectoryEntry(
                name=b"file1",
                type="file",
                perms=DentryPerms.content,
                target=cnt1.sha1_git,
            ),
            DirectoryEntry(
                name=b"file2",
                type="file",
                perms=DentryPerms.content,
                target=cnt2.sha1_git,
            ),
        )
    )
    dir3 = Directory(
        entries=(
            DirectoryEntry(
                name=b"file3",
                type="file",
                perms=DentryPerms.content,
                target=cnt3.sha1_git,
            ),
        )
    )
    dir4 = Directory(
        entries=(
            DirectoryEntry(
                name=b"directory3",
                type="dir",
                perms=DentryPerms.directory,
                target=dir3.id,
            ),
        )
    )
    rev1 = Revision(
        message=b"msg1",
        date=date,
        committer_date=date,
        author=author,
        committer=author,
        directory=dir1.id,
        type=RevisionType.GIT,
        synthetic=True,
    )
    rev2 = Revision(
        message=b"msg2",
        date=date,
        committer_date=date,
        author=author,
        committer=author,
        directory=dir2.id,
        parents=(rev1.id,),
        type=RevisionType.GIT,
        synthetic=True,
    )

    rel2 = Release(
        name=b"1.0.0",
        message=b"tag2",
        target_type=ObjectType.REVISION,
        target=rev2.id,
        synthetic=True,
    )
    rel3 = Release(
        name=b"1.0.0-blob",
        message=b"tagged-blob",
        target_type=ObjectType.CONTENT,
        target=cnt5.sha1_git,
        synthetic=True,
    )
    rel4 = Release(
        name=b"1.0.0-weird",
        message=b"weird release",
        target_type=ObjectType.RELEASE,
        target=rel3.id,
        synthetic=True,
    )
    rel5 = Release(
        name=b"1.0.0:weirdname",
        message=b"weird release",
        target_type=ObjectType.RELEASE,
        target=rel2.id,
        synthetic=True,
    )

    # Create snapshot:

    branches = {
        b"refs/heads/master": SnapshotBranch(
            target=rev2.id, target_type=SnapshotTargetType.REVISION
        ),
    }
    if tag:
        branches[b"refs/tags/1.0.0"] = SnapshotBranch(
            target=rel2.id, target_type=SnapshotTargetType.RELEASE
        )
    if weird_branches:
        branches[b"refs/heads/tree-ref"] = SnapshotBranch(
            target=dir4.id, target_type=SnapshotTargetType.DIRECTORY
        )
        branches[b"refs/heads/blob-ref"] = SnapshotBranch(
            target=cnt4.sha1_git, target_type=SnapshotTargetType.CONTENT
        )
        branches[b"refs/tags/1.0.0-weird"] = SnapshotBranch(
            target=rel4.id, target_type=SnapshotTargetType.RELEASE
        )
    snp = Snapshot(branches=branches)

    # "Fill" swh-graph

    if up_to_date_graph:
        nodes = [cnt1, cnt2, dir1, dir2, rev1, rev2, snp]
        edges = [
            (dir1, cnt1),
            (dir2, cnt1),
            (dir2, cnt2),
            (rev1, dir1),
            (rev2, dir2),
            (rev2, rev1),
            (snp, rev2),
        ]
        if tag:
            nodes.append(rel2)
            edges.append((rel2, rev2))
            edges.append((snp, rel2))
        if weird_branches:
            nodes.extend([cnt3, cnt4, cnt5, dir3, dir4, rel3, rel4, rel5])
            edges.extend(
                [
                    (dir3, cnt3),
                    (dir4, dir3),
                    (snp, dir4),
                    (snp, cnt4),
                    (snp, rel4),
                    (rel4, rel3),
                    (rel3, cnt5),
                    (rel5, rev2),
                ]
            )
    else:
        nodes = [cnt1, cnt2, cnt3, dir1, dir2, dir3, rev1]
        edges = [
            (dir1, cnt1),
            (dir2, cnt1),
            (dir2, cnt2),
            (dir3, cnt3),
            (rev1, dir1),
        ]
        if tag:
            nodes.append(rel2)
        if weird_branches:
            nodes.extend([cnt3, dir3])
            edges.extend([(dir3, cnt3)])

    nodes = [str(n.swhid()) for n in nodes]
    edges = [(str(s.swhid()), str(d.swhid())) for (s, d) in edges]

    # Add all objects to storage
    swh_storage.content_add([cnt1, cnt2, cnt3, cnt4, cnt5])
    swh_storage.directory_add([dir1, dir2, dir3, dir4])
    swh_storage.revision_add([rev1, rev2])
    swh_storage.release_add([rel2, rel3, rel4, rel5])
    swh_storage.snapshot_add([snp])

    # Add spy on swh_storage, to make sure revision_log is not called
    # (the graph must be used instead)
    swh_storage = unittest.mock.MagicMock(wraps=swh_storage)

    # Add all objects to graph
    swh_graph = unittest.mock.Mock(wraps=GraphClient(nodes=nodes, edges=edges))

    # Cook
    backend = InMemoryVaultBackend()
    cooked_swhid = {
        RootObjects.SNAPSHOT: snp.swhid(),
        RootObjects.REVISION: rev2.swhid(),
        RootObjects.RELEASE: rel2.swhid(),
        RootObjects.WEIRD_RELEASE: rel5.swhid(),
    }[root_object]
    cooker = GitBareCooker(
        cooked_swhid,
        backend=backend,
        storage=swh_storage,
        graph=swh_graph,
    )

    if weird_branches:
        # git-fsck now rejects refs pointing to trees and blobs,
        # but some old git repos have them.
        cooker.use_fsck = False

    cooker.cook()

    # Get bundle
    bundle = backend.fetch("git_bare", cooked_swhid)

    # Extract bundle and make sure both revisions are in it
    with tempfile.TemporaryDirectory("swh-vault-test-bare") as tempdir:
        with tarfile.open(fileobj=io.BytesIO(bundle)) as tf:
            tf.extractall(tempdir)

        if root_object != RootObjects.WEIRD_RELEASE:
            # check master ref exists in repository
            master_ref_path = os.path.join(
                tempdir, f"{cooked_swhid}.git/refs/heads/master"
            )
            assert os.path.exists(master_ref_path)
            with open(master_ref_path, "r") as master_ref:
                assert master_ref.read() == branches[b"refs/heads/master"].target.hex()

        if root_object in (RootObjects.SNAPSHOT, RootObjects.REVISION):
            log_head = "master"
        elif root_object == RootObjects.RELEASE:
            log_head = "1.0.0"
        elif root_object == RootObjects.WEIRD_RELEASE:
            log_head = "release"
        else:
            assert False, root_object

        output = subprocess.check_output(
            [
                "git",
                "-C",
                f"{tempdir}/{cooked_swhid}.git",
                "log",
                "--format=oneline",
                "--decorate=",
                log_head,
            ]
        )

        assert output.decode() == f"{rev2.id.hex()} msg2\n{rev1.id.hex()} msg1\n"

    # Make sure the graph was used instead of swh_storage.revision_log
    if root_object == RootObjects.SNAPSHOT:
        if up_to_date_graph:
            # The graph has everything, so the first call succeeds and returns
            # all objects transitively pointed by the snapshot
            swh_graph.visit_nodes.assert_has_calls(
                [
                    unittest.mock.call(str(snp.swhid()), edges="snp:*,rel:*,rev:rev"),
                ]
            )
        else:
            # The graph does not have everything, so the first call returns nothing.
            # However, the second call (on the top rev) succeeds and returns
            # all objects but the rev and the rel
            swh_graph.visit_nodes.assert_has_calls(
                [
                    unittest.mock.call(str(snp.swhid()), edges="snp:*,rel:*,rev:rev"),
                    unittest.mock.call(str(rev2.swhid()), edges="rev:rev"),
                ]
            )
    elif root_object in (
        RootObjects.REVISION,
        RootObjects.RELEASE,
        RootObjects.WEIRD_RELEASE,
    ):
        swh_graph.visit_nodes.assert_has_calls(
            [unittest.mock.call(str(rev2.swhid()), edges="rev:rev")]
        )
    else:
        assert False, root_object

    if up_to_date_graph:
        swh_storage.revision_log.assert_not_called()
        swh_storage.revision_shortlog.assert_not_called()
    else:
        swh_storage.revision_log.assert_called()


@pytest.mark.parametrize(
    "mismatch_on", ["content", "directory", "revision1", "revision2", "none"]
)
def test_checksum_mismatch(swh_storage, mismatch_on):
    date = TimestampWithTimezone.from_datetime(
        datetime.datetime(2021, 5, 7, 8, 43, 59, tzinfo=datetime.timezone.utc)
    )
    author = Person.from_fullname(b"Foo <foo@example.org>")

    wrong_hash = b"\x12\x34" * 10

    cnt1 = Content.from_data(b"Tr0ub4dor&3")
    if mismatch_on == "content":
        cnt1 = attr.evolve(cnt1, sha1_git=wrong_hash)

    dir1 = Directory(
        entries=(
            DirectoryEntry(
                name=b"file1",
                type="file",
                perms=DentryPerms.content,
                target=cnt1.sha1_git,
            ),
        )
    )

    if mismatch_on == "directory":
        dir1 = attr.evolve(dir1, id=wrong_hash)

    rev1 = Revision(
        message=b"msg1",
        date=date,
        committer_date=date,
        author=author,
        committer=author,
        directory=dir1.id,
        type=RevisionType.GIT,
        synthetic=True,
    )

    if mismatch_on == "revision1":
        rev1 = attr.evolve(rev1, id=wrong_hash)

    rev2 = Revision(
        message=b"msg2",
        date=date,
        committer_date=date,
        author=author,
        committer=author,
        directory=dir1.id,
        parents=(rev1.id,),
        type=RevisionType.GIT,
        synthetic=True,
    )

    if mismatch_on == "revision2":
        rev2 = attr.evolve(rev2, id=wrong_hash)

    cooked_swhid = rev2.swhid()

    swh_storage.content_add([cnt1])
    swh_storage.directory_add([dir1])
    swh_storage.revision_add([rev1, rev2])

    backend = InMemoryVaultBackend()
    cooker = GitBareCooker(
        cooked_swhid,
        backend=backend,
        storage=swh_storage,
        graph=None,
    )

    cooker.cook()

    # Get bundle
    bundle = backend.fetch("git_bare", cooked_swhid)

    # Extract bundle and make sure both revisions are in it
    with tempfile.TemporaryDirectory("swh-vault-test-bare") as tempdir:
        with tarfile.open(fileobj=io.BytesIO(bundle)) as tf:
            tf.extractall(tempdir)

        if mismatch_on != "revision2":
            # git-log fails if the head revision is corrupted
            # TODO: we need to find a way to make this somewhat usable
            output = subprocess.check_output(
                [
                    "git",
                    "-C",
                    f"{tempdir}/{cooked_swhid}.git",
                    "log",
                    "--format=oneline",
                    "--decorate=",
                ]
            )

            assert output.decode() == f"{rev2.id.hex()} msg2\n{rev1.id.hex()} msg1\n"
