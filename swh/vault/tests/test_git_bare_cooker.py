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
import io
import subprocess
import tarfile
import tempfile
import unittest.mock

import pytest

from swh.graph.naive_client import NaiveClient as GraphClient
from swh.model.from_disk import DentryPerms
from swh.model.model import (
    Content,
    Directory,
    DirectoryEntry,
    Person,
    Revision,
    RevisionType,
    TimestampWithTimezone,
)
from swh.vault.cookers.git_bare import GitBareCooker
from swh.vault.in_memory_backend import InMemoryVaultBackend


def get_objects(last_revision_in_graph):
    """
    Build objects::

          rev1  <------ rev2
           |             |
           v             v
          dir1          dir2
           |           /   |
           v          /    v
          cnt1  <----°    cnt2
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

    if last_revision_in_graph:
        nodes = [str(n.swhid()) for n in [cnt1, cnt2, dir1, dir2, rev1, rev2]]
        edges = [
            (str(s.swhid()), str(d.swhid()))
            for (s, d) in [
                (dir1, cnt1),
                (dir2, cnt1),
                (dir2, cnt2),
                (rev1, dir1),
                (rev2, dir2),
                (rev2, rev1),
            ]
        ]
    else:
        nodes = [str(n.swhid()) for n in [cnt1, cnt2, dir1, dir2, rev1]]
        edges = [
            (str(s.swhid()), str(d.swhid()))
            for (s, d) in [(dir1, cnt1), (dir2, cnt1), (dir2, cnt2), (rev1, dir1),]
        ]

    return (cnt1, cnt2, dir1, dir2, rev1, rev2, nodes, edges)


@pytest.mark.parametrize("last_revision_in_graph", [True, False])
def test_graph_revisions(swh_storage, last_revision_in_graph):
    (cnt1, cnt2, dir1, dir2, rev1, rev2, nodes, edges) = get_objects(
        last_revision_in_graph
    )

    # Add all objects to storage
    swh_storage.content_add([cnt1, cnt2])
    swh_storage.directory_add([dir1, dir2])
    swh_storage.revision_add([rev1, rev2])

    # Add spy on swh_storage, to make sure revision_log is not called
    # (the graph must be used instead)
    swh_storage = unittest.mock.MagicMock(wraps=swh_storage)

    # Add all objects to graph
    swh_graph = unittest.mock.Mock(wraps=GraphClient(nodes=nodes, edges=edges))

    # Cook
    backend = InMemoryVaultBackend()
    cooker = GitBareCooker(
        "revision_gitbare",
        rev2.id,
        backend=backend,
        storage=swh_storage,
        graph=swh_graph,
    )
    cooker.cook()

    # Get bundle
    bundle = backend.fetch("revision_gitbare", rev2.id)

    # Extract bundle and make sure both revisions are in it
    with tempfile.TemporaryDirectory("swh-vault-test-bare") as tempdir:
        with tarfile.open(fileobj=io.BytesIO(bundle)) as tf:
            tf.extractall(tempdir)

        output = subprocess.check_output(
            [
                "git",
                "-C",
                f"{tempdir}/{rev2.swhid()}.git",
                "log",
                "--format=oneline",
                "--decorate=",
            ]
        )

        assert output.decode() == f"{rev2.id.hex()} msg2\n{rev1.id.hex()} msg1\n"

    # Make sure the graph was used instead of swh_storage.revision_log
    swh_graph.visit_nodes.assert_called_once_with(str(rev2.swhid()), edges="rev:rev")
    if last_revision_in_graph:
        swh_storage.revision_log.assert_not_called()
        swh_storage.revision_shortlog.assert_not_called()
    else:
        swh_storage.revision_log.assert_called()
