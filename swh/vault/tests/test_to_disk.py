# Copyright (C) 2020-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.model.from_disk import DentryPerms
from swh.model.model import Content, Directory, DirectoryEntry, SkippedContent
from swh.vault.to_disk import DirectoryBuilder, get_filtered_file_content


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
        {
            "status": "absent",
            "target": skipped_content.sha1_git,
        },
    ]

    res = [
        get_filtered_file_content(swh_storage, file_data) for file_data in files_data
    ]

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
        {
            "status": "blah",
            "target": b"c" * 20,
        },
    ]

    with pytest.raises(AssertionError, match="unexpected status 'blah'"):
        [get_filtered_file_content(swh_storage, file_data) for file_data in files_data]


def _fill_storage(swh_storage, exclude_cnt3=False, exclude_dir1=False):
    cnt1 = Content.from_data(b"foo bar")
    cnt2 = Content.from_data(b"bar baz")
    cnt3 = Content.from_data(b"baz qux")
    dir1 = Directory(
        entries=(
            DirectoryEntry(
                name=b"content1",
                type="file",
                target=cnt1.sha1_git,
                perms=DentryPerms.content,
            ),
            DirectoryEntry(
                name=b"content2",
                type="file",
                target=cnt2.sha1_git,
                perms=DentryPerms.content,
            ),
        )
    )
    dir2 = Directory(
        entries=(
            DirectoryEntry(
                name=b"content3",
                type="file",
                target=cnt3.sha1_git,
                perms=DentryPerms.content,
            ),
            DirectoryEntry(
                name=b"subdirectory",
                type="dir",
                target=dir1.id,
                perms=DentryPerms.directory,
            ),
        )
    )
    if exclude_cnt3:
        swh_storage.content_add([cnt1, cnt2])
    else:
        swh_storage.content_add([cnt1, cnt2, cnt3])
    if exclude_dir1:
        swh_storage.directory_add([dir2])
    else:
        swh_storage.directory_add([dir1, dir2])

    return dir2


@pytest.mark.parametrize(
    "use_objstorage", [False, True], ids=["use only storage", "use objstorage"]
)
def test_directory_builder(swh_storage, tmp_path, use_objstorage):
    dir2 = _fill_storage(swh_storage)

    root = tmp_path / "root"
    builder = DirectoryBuilder(
        storage=swh_storage,
        root=bytes(root),
        dir_id=dir2.id,
        objstorage=swh_storage.objstorage if use_objstorage else None,
    )

    assert not root.exists()

    builder.build()

    assert root.is_dir()
    assert set(root.glob("**/*")) == {
        root / "subdirectory",
        root / "subdirectory" / "content1",
        root / "subdirectory" / "content2",
        root / "content3",
    }

    assert (root / "subdirectory" / "content1").open().read() == "foo bar"
    assert (root / "subdirectory" / "content2").open().read() == "bar baz"
    assert (root / "content3").open().read() == "baz qux"


@pytest.mark.parametrize(
    "use_objstorage", [False, True], ids=["use only storage", "use objstorage"]
)
def test_directory_builder_missing_content(swh_storage, tmp_path, use_objstorage):
    dir2 = _fill_storage(swh_storage, exclude_cnt3=True)

    root = tmp_path / "root"
    builder = DirectoryBuilder(
        storage=swh_storage,
        root=bytes(root),
        dir_id=dir2.id,
        objstorage=swh_storage.objstorage if use_objstorage else None,
    )

    assert not root.exists()

    builder.build()

    assert root.is_dir()

    assert "This content is missing" in (root / "content3").open().read()


@pytest.mark.parametrize(
    "use_objstorage", [False, True], ids=["use only storage", "use objstorage"]
)
def test_directory_builder_missing_directory(swh_storage, tmp_path, use_objstorage):
    dir2 = _fill_storage(swh_storage, exclude_dir1=True)

    root = tmp_path / "root"
    builder = DirectoryBuilder(
        storage=swh_storage,
        root=bytes(root),
        dir_id=dir2.id,
        objstorage=swh_storage.objstorage if use_objstorage else None,
    )

    assert not root.exists()

    builder.build()

    assert root.is_dir()
    assert set(root.glob("**/*")) == {
        root / "subdirectory",
        root / "content3",
    }

    assert (root / "content3").open().read() == "baz qux"
