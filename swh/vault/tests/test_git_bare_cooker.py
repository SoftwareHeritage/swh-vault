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
import subprocess
import tarfile
import tempfile
import unittest.mock

import attr
import dulwich.repo
import pytest
from pytest import param
from pytest_postgresql import factories

from swh.core.db.pytest_plugin import initialize_database_for_module
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
    TargetType,
    Timestamp,
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
    return get_storage("local", db=storage_postgresql.dsn, objstorage={"cls": "memory"})


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
    from swh.graph.naive_client import NaiveClient as GraphClient

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
            target=rev2.id, target_type=TargetType.REVISION
        ),
    }
    if tag:
        branches[b"refs/tags/1.0.0"] = SnapshotBranch(
            target=rel2.id, target_type=TargetType.RELEASE
        )
    if weird_branches:
        branches[b"refs/heads/tree-ref"] = SnapshotBranch(
            target=dir4.id, target_type=TargetType.DIRECTORY
        )
        branches[b"refs/heads/blob-ref"] = SnapshotBranch(
            target=cnt4.sha1_git, target_type=TargetType.CONTENT
        )
        branches[b"refs/tags/1.0.0-weird"] = SnapshotBranch(
            target=rel4.id, target_type=TargetType.RELEASE
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


@pytest.mark.parametrize(
    "use_graph",
    [
        pytest.param(False, id="without-graph"),
        pytest.param(True, id="with-graph", marks=pytest.mark.graph),
    ],
)
def test_ignore_displayname(swh_storage, use_graph):
    """Tests the original authorship information is used instead of
    configured display names; otherwise objects would not match their hash,
    and git-fsck/git-clone would fail.

    This tests both with and without swh-graph, as both configurations use different
    code paths to fetch revisions.
    """

    date = TimestampWithTimezone.from_numeric_offset(Timestamp(1643882820, 0), 0, False)
    legacy_person = Person.from_fullname(b"old me <old@example.org>")
    current_person = Person.from_fullname(b"me <me@example.org>")

    content = Content.from_data(b"foo")
    swh_storage.content_add([content])

    directory = Directory(
        entries=(
            DirectoryEntry(
                name=b"file1", type="file", perms=0o100644, target=content.sha1_git
            ),
        ),
    )
    swh_storage.directory_add([directory])

    revision = Revision(
        message=b"rev",
        author=legacy_person,
        date=date,
        committer=legacy_person,
        committer_date=date,
        parents=(),
        type=RevisionType.GIT,
        directory=directory.id,
        synthetic=True,
    )
    swh_storage.revision_add([revision])

    release = Release(
        name=b"v1.1.0",
        message=None,
        author=legacy_person,
        date=date,
        target=revision.id,
        target_type=ObjectType.REVISION,
        synthetic=True,
    )
    swh_storage.release_add([release])

    snapshot = Snapshot(
        branches={
            b"refs/tags/v1.1.0": SnapshotBranch(
                target=release.id, target_type=TargetType.RELEASE
            ),
            b"HEAD": SnapshotBranch(
                target=revision.id, target_type=TargetType.REVISION
            ),
        }
    )
    swh_storage.snapshot_add([snapshot])

    # Add all objects to graph
    if use_graph:
        from swh.graph.naive_client import NaiveClient as GraphClient

        nodes = [
            str(x.swhid()) for x in [content, directory, revision, release, snapshot]
        ]
        edges = [
            (str(x.swhid()), str(y.swhid()))
            for (x, y) in [
                (directory, content),
                (revision, directory),
                (release, revision),
                (snapshot, release),
                (snapshot, revision),
            ]
        ]
        swh_graph = unittest.mock.Mock(wraps=GraphClient(nodes=nodes, edges=edges))
    else:
        swh_graph = None

    # Set a display name
    with swh_storage.db() as db:
        with db.transaction() as cur:
            cur.execute(
                "UPDATE person set displayname = %s where fullname = %s",
                (current_person.fullname, legacy_person.fullname),
            )

    # Check the display name did apply in the storage
    assert swh_storage.revision_get([revision.id])[0] == attr.evolve(
        revision,
        author=current_person,
        committer=current_person,
    )

    # Cook
    cooked_swhid = snapshot.swhid()
    backend = InMemoryVaultBackend()
    cooker = GitBareCooker(
        cooked_swhid,
        backend=backend,
        storage=swh_storage,
        graph=swh_graph,
    )

    cooker.cook()

    # Get bundle
    bundle = backend.fetch("git_bare", cooked_swhid)

    # Extract bundle and make sure both revisions are in it
    with tempfile.TemporaryDirectory("swh-vault-test-bare") as tempdir:
        with tarfile.open(fileobj=io.BytesIO(bundle)) as tf:
            tf.extractall(tempdir)

        # If we are here, it means git-fsck succeeded when called by cooker.cook(),
        # so we already know the original person was used. Let's double-check.

        repo = dulwich.repo.Repo(f"{tempdir}/{cooked_swhid}.git")

        tag = repo[b"refs/tags/v1.1.0"]
        assert tag.tagger == legacy_person.fullname

        commit = repo[tag.object[1]]
        assert commit.author == legacy_person.fullname
