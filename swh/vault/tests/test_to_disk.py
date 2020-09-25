# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.model.model import Content, SkippedContent
from swh.vault.to_disk import get_filtered_files_content


def test_get_filtered_files_content(swh_storage):
    content = Content.from_data(b"foo bar")
    skipped_content = SkippedContent(
        sha1=None,
        sha1_git=b"c" * 20,
        sha256=None,
        blake2s256=None,
        length=42,
        status="absent",
        reason="for some reason",
    )
    swh_storage.content_add([content])
    swh_storage.skipped_content_add([skipped_content])

    files_data = [
        {
            "status": "visible",
            "sha1": content.sha1,
            "sha1_git": content.sha1_git,
            "target": content.sha1_git,
        },
        {"status": "absent", "target": skipped_content.sha1_git,},
    ]

    res = list(get_filtered_files_content(swh_storage, files_data))

    assert res == [
        {
            "content": content.data,
            "status": "visible",
            "sha1": content.sha1,
            "sha1_git": content.sha1_git,
            "target": content.sha1_git,
        },
        {
            "content": (
                b"This content has not been retrieved in the "
                b"Software Heritage archive due to its size."
            ),
            "status": "absent",
            "target": skipped_content.sha1_git,
        },
    ]


def test_get_filtered_files_content__unknown_status(swh_storage):
    content = Content.from_data(b"foo bar")
    swh_storage.content_add([content])

    files_data = [
        {
            "status": "visible",
            "sha1": content.sha1,
            "sha1_git": content.sha1_git,
            "target": content.sha1_git,
        },
        {"status": None, "target": b"c" * 20,},
    ]

    with pytest.raises(AssertionError, match="unexpected status None"):
        list(get_filtered_files_content(swh_storage, files_data))
