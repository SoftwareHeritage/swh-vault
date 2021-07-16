# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""
This module contains additional tests for the bare cooker.
Generic cooker tests (eg. without swh-graph) in test_cookers.py also
run on the bare cooker.
"""

import datetime
import glob
import io
import itertools
import subprocess
import tarfile
import tempfile
import unittest.mock

import pytest

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
    TimestampWithTimezone,
)
from swh.vault.cookers.git_bare import GitBareCooker
from swh.vault.in_memory_backend import InMemoryVaultBackend


def get_objects(up_to_date_graph, release):
    """
    Build objects::

                        rel2
                         |
                         v
          rev1  <------ rev2
           |             |
           v             v
          dir1          dir2
           |           /   |
           v          /    v
          cnt1  <----°    cnt2

    If up_to_date_graph is true, then swh-graph contains all objects.
    Else, rev2, rel2, and the snapshot are missing from the graph.
    """
    date = TimestampWithTimezone.from_datetime(
        datetime.datetime(2021, 5, 7, 8, 43, 59, tzinfo=datetime.timezone.utc)
    )
    author = Person.from_fullname(b"Foo <foo@example.org>")
    cnt1 = Content.from_data(b"hello")
    cnt2 = Content.from_data(b"world")
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

    branches = {
        b"refs/heads/master": SnapshotBranch(
            target=rev2.id, target_type=TargetType.REVISION
        )
    }
    if release:
        branches[b"refs/tags/1.0.0"] = SnapshotBranch(
            target=rel2.id, target_type=TargetType.RELEASE
        )
    snp = Snapshot(branches=branches)

    if up_to_date_graph:
        nodes = [str(n.swhid()) for n in [cnt1, cnt2, dir1, dir2, rev1, rev2, snp]]
        edges = [
            (dir1, cnt1),
            (dir2, cnt1),
            (dir2, cnt2),
            (rev1, dir1),
            (rev2, dir2),
            (rev2, rev1),
            (snp, rev2),
        ]
        if release:
            nodes.append(str(rel2.swhid()))
            edges.append((rel2, rev2))
            edges.append((snp, rel2))
    else:
        nodes = [str(n.swhid()) for n in [cnt1, cnt2, dir1, dir2, rev1]]
        if release:
            nodes.append(str(rel2.swhid()))
        edges = [
            (dir1, cnt1),
            (dir2, cnt1),
            (dir2, cnt2),
            (rev1, dir1),
        ]

    edges = [(str(s.swhid()), str(d.swhid())) for (s, d) in edges]

    return (cnt1, cnt2, dir1, dir2, rev1, rev2, rel2, snp, nodes, edges)


@pytest.mark.graph
@pytest.mark.parametrize(
    "snapshot,up_to_date_graph,release",
    list(itertools.product([False], [True, False], [False]))  # no snp implies no rel
    + list(itertools.product([True], [True, False], [True, False])),
)
def test_graph_revisions(swh_storage, up_to_date_graph, snapshot, release):
    from swh.graph.naive_client import NaiveClient as GraphClient

    (cnt1, cnt2, dir1, dir2, rev1, rev2, rel2, snp, nodes, edges) = get_objects(
        up_to_date_graph, release=release,
    )

    # Add all objects to storage
    swh_storage.content_add([cnt1, cnt2])
    swh_storage.directory_add([dir1, dir2])
    swh_storage.revision_add([rev1, rev2])
    swh_storage.release_add([rel2])
    swh_storage.snapshot_add([snp])

    # Add spy on swh_storage, to make sure revision_log is not called
    # (the graph must be used instead)
    swh_storage = unittest.mock.MagicMock(wraps=swh_storage)

    # Add all objects to graph
    swh_graph = unittest.mock.Mock(wraps=GraphClient(nodes=nodes, edges=edges))

    # Cook
    backend = InMemoryVaultBackend()
    if snapshot:
        cooker_name = "snapshot_gitbare"
        cooked_id = snp.id
    else:
        cooker_name = "revision_gitbare"
        cooked_id = rev2.id
    cooker = GitBareCooker(
        cooker_name, cooked_id, backend=backend, storage=swh_storage, graph=swh_graph,
    )
    cooker.cook()

    # Get bundle
    bundle = backend.fetch(cooker_name, cooked_id)

    # Extract bundle and make sure both revisions are in it
    with tempfile.TemporaryDirectory("swh-vault-test-bare") as tempdir:
        with tarfile.open(fileobj=io.BytesIO(bundle)) as tf:
            tf.extractall(tempdir)

        output = subprocess.check_output(
            [
                "git",
                "-C",
                glob.glob(f"{tempdir}/*{cooked_id.hex()}.git")[0],
                "log",
                "--format=oneline",
                "--decorate=",
            ]
        )

        assert output.decode() == f"{rev2.id.hex()} msg2\n{rev1.id.hex()} msg1\n"

    # Make sure the graph was used instead of swh_storage.revision_log
    if snapshot:
        if up_to_date_graph:
            # The graph has everything, so the first call succeeds and returns
            # all objects transitively pointed by the snapshot
            swh_graph.visit_nodes.assert_has_calls(
                [unittest.mock.call(str(snp.swhid()), edges="snp:*,rel:*,rev:rev"),]
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
    else:
        swh_graph.visit_nodes.assert_has_calls(
            [unittest.mock.call(str(rev2.swhid()), edges="rev:rev")]
        )
    if up_to_date_graph:
        swh_storage.revision_log.assert_not_called()
        swh_storage.revision_shortlog.assert_not_called()
    else:
        swh_storage.revision_log.assert_called()
