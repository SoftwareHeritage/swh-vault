# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import datetime
import glob
import gzip
import io
import os
import pathlib
import shutil
import subprocess
import tarfile
import tempfile
import unittest
import unittest.mock

import attr
import dulwich.fastexport
import dulwich.index
import dulwich.objects
import dulwich.porcelain
import dulwich.repo
import pytest

from swh.loader.git.loader import GitLoader
from swh.model import from_disk, hashutil
from swh.model.model import (
    Content,
    Directory,
    DirectoryEntry,
    Person,
    Release,
    ReleaseTargetType,
    Revision,
    RevisionType,
    SkippedContent,
    Snapshot,
    SnapshotBranch,
    SnapshotTargetType,
    Timestamp,
    TimestampWithTimezone,
)
from swh.model.swhids import CoreSWHID, ObjectType
from swh.vault.cookers import DirectoryCooker, GitBareCooker, RevisionGitfastCooker
from swh.vault.tests.vault_testing import hash_content
from swh.vault.to_disk import HIDDEN_MESSAGE, SKIPPED_MESSAGE


class _TestRepo:
    """A tiny context manager for a test git repository, with some utility
    functions to perform basic git stuff.
    """

    def __init__(self, repo_dir=None):
        self.repo_dir = repo_dir

    def __enter__(self):
        if self.repo_dir:
            self.tmp_dir = None
            self.repo = dulwich.repo.Repo(self.repo_dir)
        else:
            self.tmp_dir = tempfile.TemporaryDirectory(prefix="tmp-vault-repo-")
            self.repo_dir = self.tmp_dir.__enter__()
            self.repo = dulwich.repo.Repo.init(self.repo_dir)
        self.author_name = b"Test Author"
        self.author_email = b"test@softwareheritage.org"
        self.author = b"%s <%s>" % (self.author_name, self.author_email)
        self.base_date = 258244200
        self.counter = 0
        return pathlib.Path(self.repo_dir)

    def __exit__(self, exc, value, tb):
        if self.tmp_dir is not None:
            self.tmp_dir.__exit__(exc, value, tb)
            self.repo_dir = None

    def checkout(self, rev_sha):
        rev = self.repo[rev_sha]
        dulwich.index.build_index_from_tree(
            str(self.repo_dir), self.repo.index_path(), self.repo.object_store, rev.tree
        )

    def git_shell(self, *cmd, stdout=subprocess.DEVNULL, **kwargs):
        name = self.author_name
        email = self.author_email
        date = "%d +0000" % (self.base_date + self.counter)
        env = {
            # Set git commit format
            "GIT_AUTHOR_NAME": name,
            "GIT_AUTHOR_EMAIL": email,
            "GIT_AUTHOR_DATE": date,
            "GIT_COMMITTER_NAME": name,
            "GIT_COMMITTER_EMAIL": email,
            "GIT_COMMITTER_DATE": date,
            # Ignore all the system-wide and user configurations
            "GIT_CONFIG_NOSYSTEM": "1",
            "HOME": str(self.tmp_dir),
            "XDG_CONFIG_HOME": str(self.tmp_dir),
        }
        kwargs.setdefault("env", {}).update(env)

        subprocess.check_call(
            ("git", "-C", self.repo_dir) + cmd, stdout=stdout, **kwargs
        )

    def commit(self, message="Commit test\n", ref=b"HEAD"):
        """Commit the current working tree in a new commit with message on
        the branch 'ref'.

        At the end of the commit, the reference should stay the same
        and the index should be clean.

        """
        paths = [
            os.path.relpath(path, self.repo_dir)
            for path in glob.glob(self.repo_dir + "/**/*", recursive=True)
        ]
        self.repo.stage(paths)
        message = message.encode() + b"\n"
        ret = self.repo.do_commit(
            message=message,
            committer=self.author,
            commit_timestamp=self.base_date + self.counter,
            commit_timezone=0,
            ref=ref,
        )
        self.counter += 1

        # committing on another branch leaves
        # dangling files in index
        if ref != b"HEAD":
            # XXX this should work (but does not)
            # dulwich.porcelain.reset(self.repo, 'hard')
            self.git_shell("reset", "--hard", "HEAD")
        return ret

    def tag(self, name, target=b"HEAD", message=None):
        dulwich.porcelain.tag_create(
            self.repo,
            name,
            message=message,
            annotated=message is not None,
            objectish=target,
        )

    def merge(self, parent_sha_list, message="Merge branches."):
        self.git_shell(
            "merge",
            "--allow-unrelated-histories",
            "-m",
            message,
            *[p.decode() for p in parent_sha_list],
        )
        self.counter += 1
        return self.repo.refs[b"HEAD"]

    def print_debug_graph(self, reflog=False):
        args = ["log", "--all", "--graph", "--decorate"]
        if reflog:
            args.append("--reflog")
        self.git_shell(*args, stdout=None)


@pytest.fixture
def git_loader(
    swh_storage,
):
    """Instantiate a Git Loader using the storage instance as storage."""

    def _create_loader(directory):
        return GitLoader(
            swh_storage,
            directory,
        )

    return _create_loader


@contextlib.contextmanager
def cook_extract_directory_dircooker(
    storage, swhid, fsck=True, direct_objstorage=False
):
    """Context manager that cooks a directory and extract it."""
    backend = unittest.mock.MagicMock()
    backend.storage = storage
    cooker = DirectoryCooker(
        swhid,
        backend=backend,
        storage=storage,
        objstorage=storage.objstorage if direct_objstorage else None,
    )
    cooker.fileobj = io.BytesIO()
    assert cooker.check_exists()
    cooker.prepare_bundle()
    cooker.fileobj.seek(0)
    with tempfile.TemporaryDirectory(prefix="tmp-vault-extract-") as td:
        with tarfile.open(fileobj=cooker.fileobj, mode="r") as tar:
            tar.extractall(td)
        yield pathlib.Path(td) / str(swhid)
    cooker.storage = None


@contextlib.contextmanager
def cook_extract_directory_gitfast(storage, swhid, fsck=True, direct_objstorage=False):
    """Context manager that cooks a revision containing a directory and extract it,
    using RevisionGitfastCooker"""
    test_repo = _TestRepo()
    with test_repo as p:
        date = TimestampWithTimezone.from_datetime(
            datetime.datetime.now(datetime.timezone.utc)
        )
        revision = Revision(
            directory=swhid.object_id,
            message=b"dummy message",
            author=Person.from_fullname(b"someone"),
            committer=Person.from_fullname(b"someone"),
            date=date,
            committer_date=date,
            type=RevisionType.GIT,
            synthetic=False,
        )
        storage.revision_add([revision])

    with (
        cook_stream_revision_gitfast(storage, revision.swhid()) as stream,
        test_repo as p,
    ):
        processor = dulwich.fastexport.GitImportProcessor(test_repo.repo)
        processor.import_stream(stream)
        test_repo.checkout(b"HEAD")
        shutil.rmtree(p / ".git")
        yield p


@contextlib.contextmanager
def cook_extract_directory_git_bare(storage, swhid, fsck=True, direct_objstorage=False):
    """Context manager that cooks a revision and extract it,
    using GitBareCooker"""
    backend = unittest.mock.MagicMock()
    backend.storage = storage

    # Cook the object
    cooker = GitBareCooker(
        swhid,
        backend=backend,
        storage=storage,
        objstorage=storage.objstorage if direct_objstorage else None,
    )
    cooker.use_fsck = fsck  # Some tests try edge-cases that git-fsck rejects
    cooker.fileobj = io.BytesIO()
    assert cooker.check_exists()
    cooker.prepare_bundle()
    cooker.fileobj.seek(0)

    # Extract it
    with tempfile.TemporaryDirectory(prefix="tmp-vault-extract-") as td:
        with tarfile.open(fileobj=cooker.fileobj, mode="r") as tar:
            tar.extractall(td)

        # Clone it with Dulwich
        with tempfile.TemporaryDirectory(prefix="tmp-vault-clone-") as clone_dir:
            clone_dir = pathlib.Path(clone_dir)
            subprocess.check_call(
                [
                    "git",
                    "clone",
                    os.path.join(td, f"{swhid}.git"),
                    clone_dir,
                ]
            )
            shutil.rmtree(clone_dir / ".git")
            yield clone_dir


@pytest.fixture(
    scope="module",
    params=[
        cook_extract_directory_dircooker,
        cook_extract_directory_gitfast,
        cook_extract_directory_git_bare,
    ],
)
def cook_extract_directory(request):
    """A fixture that is instantiated as either cook_extract_directory_dircooker or
    cook_extract_directory_git_bare."""
    return request.param


@contextlib.contextmanager
def cook_stream_revision_gitfast(storage, swhid):
    """Context manager that cooks a revision and stream its fastexport."""
    backend = unittest.mock.MagicMock()
    backend.storage = storage
    cooker = RevisionGitfastCooker(swhid, backend=backend, storage=storage)
    cooker.fileobj = io.BytesIO()
    assert cooker.check_exists()
    cooker.prepare_bundle()
    cooker.fileobj.seek(0)
    fastexport_stream = gzip.GzipFile(fileobj=cooker.fileobj)
    yield fastexport_stream
    cooker.storage = None


@contextlib.contextmanager
def cook_extract_revision_gitfast(storage, swhid, fsck=True):
    """Context manager that cooks a revision and extract it,
    using RevisionGitfastCooker"""
    test_repo = _TestRepo()
    with cook_stream_revision_gitfast(storage, swhid) as stream, test_repo as p:
        processor = dulwich.fastexport.GitImportProcessor(test_repo.repo)
        processor.import_stream(stream)
        yield test_repo, p


@contextlib.contextmanager
def cook_extract_git_bare(storage, swhid, fsck=True):
    """Context manager that cooks a revision and extract it,
    using GitBareCooker"""
    backend = unittest.mock.MagicMock()
    backend.storage = storage

    # Cook the object
    cooker = GitBareCooker(swhid, backend=backend, storage=storage)
    cooker.use_fsck = fsck  # Some tests try edge-cases that git-fsck rejects
    cooker.fileobj = io.BytesIO()
    assert cooker.check_exists()
    cooker.prepare_bundle()
    cooker.fileobj.seek(0)

    # Extract it
    with tempfile.TemporaryDirectory(prefix="tmp-vault-extract-") as td:
        with tarfile.open(fileobj=cooker.fileobj, mode="r") as tar:
            tar.extractall(td)

        # Clone it with Dulwich
        with tempfile.TemporaryDirectory(prefix="tmp-vault-clone-") as clone_dir:
            clone_dir = pathlib.Path(clone_dir)
            subprocess.check_call(
                [
                    "git",
                    "clone",
                    os.path.join(td, f"{swhid}.git"),
                    clone_dir,
                ]
            )
            test_repo = _TestRepo(clone_dir)
            with test_repo:
                yield test_repo, clone_dir


@contextlib.contextmanager
def cook_extract_revision_git_bare(storage, swhid, fsck=True):
    with cook_extract_git_bare(
        storage,
        swhid,
        fsck=fsck,
    ) as res:
        yield res


@pytest.fixture(
    scope="module",
    params=[cook_extract_revision_gitfast, cook_extract_revision_git_bare],
)
def cook_extract_revision(request):
    """A fixture that is instantiated as either cook_extract_revision_gitfast or
    cook_extract_revision_git_bare."""
    return request.param


@contextlib.contextmanager
def cook_extract_snapshot_git_bare(storage, swhid, fsck=True):
    with cook_extract_git_bare(
        storage,
        swhid,
        fsck=fsck,
    ) as res:
        yield res


@pytest.fixture(
    scope="module",
    params=[cook_extract_snapshot_git_bare],
)
def cook_extract_snapshot(request):
    """Equivalent to cook_extract_snapshot_git_bare; but analogous to
    cook_extract_revision in case we ever have more cookers supporting snapshots"""
    return request.param


TEST_CONTENT = (
    "   test content\n" "and unicode \N{BLACK HEART SUIT}\n" " and trailing spaces   "
)
TEST_EXECUTABLE = b"\x42\x40\x00\x00\x05"


class TestDirectoryCooker:
    @pytest.mark.parametrize(
        "direct_objstorage", [True, False], ids=["use objstorage", "storage only"]
    )
    def test_directory_simple(
        self, git_loader, cook_extract_directory, direct_objstorage
    ):
        repo = _TestRepo()
        with repo as rp:
            (rp / "file").write_text(TEST_CONTENT)
            (rp / "executable").write_bytes(TEST_EXECUTABLE)
            (rp / "executable").chmod(0o755)
            (rp / "link").symlink_to("file")
            (rp / "dir1/dir2").mkdir(parents=True)
            (rp / "dir1/dir2/file").write_text(TEST_CONTENT)
            c = repo.commit()
            loader = git_loader(str(rp))
            loader.load()

            obj_id_hex = repo.repo[c].tree.decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            swhid = CoreSWHID(object_type=ObjectType.DIRECTORY, object_id=obj_id)

        with cook_extract_directory(
            loader.storage, swhid, direct_objstorage=direct_objstorage
        ) as p:
            assert (p / "file").stat().st_mode == 0o100644
            assert (p / "file").read_text() == TEST_CONTENT
            assert (p / "executable").stat().st_mode == 0o100755
            assert (p / "executable").read_bytes() == TEST_EXECUTABLE
            assert (p / "link").is_symlink()
            assert os.readlink(str(p / "link")) == "file"
            assert (p / "dir1/dir2/file").stat().st_mode == 0o100644
            assert (p / "dir1/dir2/file").read_text() == TEST_CONTENT

            directory = from_disk.Directory.from_disk(path=bytes(p))
            assert obj_id_hex == hashutil.hash_to_hex(directory.hash)

    @pytest.mark.parametrize(
        "direct_objstorage", [True, False], ids=["use objstorage", "storage only"]
    )
    def test_directory_filtered_objects(
        self, git_loader, cook_extract_directory, direct_objstorage
    ):
        repo = _TestRepo()
        with repo as rp:
            file_1, id_1 = hash_content(b"test1")
            file_2, id_2 = hash_content(b"test2")
            file_3, id_3 = hash_content(b"test3")

            (rp / "file").write_bytes(file_1)
            (rp / "hidden_file").write_bytes(file_2)
            (rp / "absent_file").write_bytes(file_3)

            c = repo.commit()
            loader = git_loader(str(rp))
            loader.load()

            obj_id_hex = repo.repo[c].tree.decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            swhid = CoreSWHID(object_type=ObjectType.DIRECTORY, object_id=obj_id)

        # alter the content of the storage
        # 1/ make file 2 an hidden file object
        loader.storage._allow_overwrite = True
        cnt2 = attr.evolve(
            loader.storage.content_get([id_2])[0], status="hidden", data=file_2
        )
        loader.storage.content_add([cnt2])
        assert loader.storage.content_get([id_2])[0].status == "hidden"

        # 2/ make file 3 an skipped file object
        cnt3 = loader.storage.content_get([id_3])[0].to_dict()
        cnt3["status"] = "absent"
        cnt3["reason"] = "no reason"
        sk_cnt3 = SkippedContent.from_dict(cnt3)
        loader.storage.skipped_content_add([sk_cnt3])
        # dirty dirty dirty... let's pretend it is the equivalent of writing sql
        # queries in the postgresql backend
        for hashkey in loader.storage._cql_runner._content_indexes:
            loader.storage._cql_runner._content_indexes[hashkey].pop(cnt3[hashkey])

        with cook_extract_directory(
            loader.storage, swhid, direct_objstorage=direct_objstorage
        ) as p:
            assert (p / "file").read_bytes() == b"test1"
            assert (p / "hidden_file").read_bytes() == HIDDEN_MESSAGE
            assert (p / "absent_file").read_bytes() == SKIPPED_MESSAGE

    @pytest.mark.parametrize(
        "direct_objstorage", [True, False], ids=["use objstorage", "storage only"]
    )
    def test_directory_bogus_perms(
        self, git_loader, cook_extract_directory, direct_objstorage
    ):
        # Some early git repositories have 664/775 permissions... let's check
        # if all the weird modes are properly normalized in the directory
        # cooker.
        repo = _TestRepo()
        with repo as rp:
            (rp / "file").write_text(TEST_CONTENT)
            (rp / "file").chmod(0o664)
            (rp / "executable").write_bytes(TEST_EXECUTABLE)
            (rp / "executable").chmod(0o775)
            (rp / "wat").write_text(TEST_CONTENT)
            (rp / "wat").chmod(0o604)

            # Disable mode cleanup
            with unittest.mock.patch("dulwich.index.cleanup_mode", lambda mode: mode):
                c = repo.commit()

            # Make sure Dulwich didn't normalize the permissions itself.
            # (if it did, then the test can't check the cooker normalized them)
            tree_id = repo.repo[c].tree
            assert {entry.mode for entry in repo.repo[tree_id].items()} == {
                0o100775,
                0o100664,
                0o100604,
            }

            # Disable mode checks
            with unittest.mock.patch("dulwich.objects.Tree.check", lambda self: None):
                loader = git_loader(str(rp))
                loader.load()

            # Make sure swh-loader didn't normalize them either
            dir_entries = loader.storage.directory_ls(hashutil.bytehex_to_hash(tree_id))
            assert {entry["perms"] for entry in dir_entries} == {
                0o100664,
                0o100775,
                0o100604,
            }

            obj_id_hex = repo.repo[c].tree.decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            swhid = CoreSWHID(object_type=ObjectType.DIRECTORY, object_id=obj_id)

        with cook_extract_directory(
            loader.storage, swhid, direct_objstorage=direct_objstorage
        ) as p:
            assert (p / "file").stat().st_mode == 0o100644
            assert (p / "executable").stat().st_mode == 0o100755
            assert (p / "wat").stat().st_mode == 0o100644

    @pytest.mark.parametrize("direct_objstorage", [True, False])
    def test_directory_objstorage(
        self, swh_storage, git_loader, mocker, direct_objstorage
    ):
        """Like test_directory_simple, but using swh_objstorage directly, without
        going through swh_storage.content_get_data()"""
        repo = _TestRepo()
        with repo as rp:
            (rp / "file").write_text(TEST_CONTENT)
            (rp / "executable").write_bytes(TEST_EXECUTABLE)
            (rp / "executable").chmod(0o755)
            (rp / "link").symlink_to("file")
            (rp / "dir1/dir2").mkdir(parents=True)
            (rp / "dir1/dir2/file").write_text(TEST_CONTENT)
            c = repo.commit()
            loader = git_loader(str(rp))
            loader.load()

            obj_id_hex = repo.repo[c].tree.decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            swhid = CoreSWHID(object_type=ObjectType.DIRECTORY, object_id=obj_id)

        # Set-up spies
        storage_content_get_data = mocker.patch.object(
            swh_storage, "content_get_data", wraps=swh_storage.content_get_data
        )
        objstorage_content_batch = mocker.patch.object(
            swh_storage.objstorage, "get", wraps=swh_storage.objstorage.get
        )

        with cook_extract_directory_git_bare(
            loader.storage, swhid, direct_objstorage=direct_objstorage
        ) as p:
            assert (p / "file").stat().st_mode == 0o100644
            assert (p / "file").read_text() == TEST_CONTENT
            assert (p / "executable").stat().st_mode == 0o100755
            assert (p / "executable").read_bytes() == TEST_EXECUTABLE
            assert (p / "link").is_symlink()
            assert os.readlink(str(p / "link")) == "file"
            assert (p / "dir1/dir2/file").stat().st_mode == 0o100644
            assert (p / "dir1/dir2/file").read_text() == TEST_CONTENT

            directory = from_disk.Directory.from_disk(path=bytes(p))
            assert obj_id_hex == hashutil.hash_to_hex(directory.hash)

        if direct_objstorage:
            storage_content_get_data.assert_not_called()
            objstorage_content_batch.assert_called()
        else:
            storage_content_get_data.assert_called()
            objstorage_content_batch.assert_not_called()

    def test_directory_revision_data(self, swh_storage):
        target_rev = "0e8a3ad980ec179856012b7eecf4327e99cd44cd"

        dir = Directory(
            entries=(
                DirectoryEntry(
                    name=b"submodule",
                    type="rev",
                    target=hashutil.hash_to_bytes(target_rev),
                    perms=0o100644,
                ),
            ),
        )
        swh_storage.directory_add([dir])

        with cook_extract_directory_dircooker(
            swh_storage, dir.swhid(), fsck=False
        ) as p:
            assert (p / "submodule").is_dir()
            assert list((p / "submodule").iterdir()) == []


class RepoFixtures:
    """Shared loading and checking methods that can be reused by different types
    of tests."""

    def load_repo_simple(self, git_loader):
        #
        #     1--2--3--4--5--6--7
        #
        repo = _TestRepo()
        with repo as rp:
            (rp / "file1").write_text(TEST_CONTENT)
            repo.commit("add file1")
            (rp / "file2").write_text(TEST_CONTENT)
            repo.commit("add file2")
            (rp / "dir1/dir2").mkdir(parents=True)
            (rp / "dir1/dir2/file").write_text(TEST_CONTENT)

            (rp / "bin1").write_bytes(TEST_EXECUTABLE)
            (rp / "bin1").chmod(0o755)
            repo.commit("add bin1")
            (rp / "link1").symlink_to("file1")
            repo.commit("link link1 to file1")
            (rp / "file2").unlink()
            repo.commit("remove file2")
            (rp / "bin1").rename(rp / "bin")
            repo.commit("rename bin1 to bin")
            loader = git_loader(str(rp))
            loader.load()
            obj_id_hex = repo.repo.refs[b"HEAD"].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=obj_id)
        return (loader, swhid)

    def check_revision_simple(self, ert, p, swhid):
        ert.checkout(b"HEAD")
        assert (p / "file1").stat().st_mode == 0o100644
        assert (p / "file1").read_text() == TEST_CONTENT
        assert (p / "link1").is_symlink()
        assert os.readlink(str(p / "link1")) == "file1"
        assert (p / "bin").stat().st_mode == 0o100755
        assert (p / "bin").read_bytes() == TEST_EXECUTABLE
        assert (p / "dir1/dir2/file").read_text() == TEST_CONTENT
        assert (p / "dir1/dir2/file").stat().st_mode == 0o100644
        assert ert.repo.refs[b"HEAD"].decode() == swhid.object_id.hex()

    def load_repo_two_roots(self, git_loader):
        #
        #    1----3---4
        #        /
        #   2----
        #
        repo = _TestRepo()
        with repo as rp:
            (rp / "file1").write_text(TEST_CONTENT)
            c1 = repo.commit("Add file1")
            del repo.repo.refs[b"refs/heads/master"]  # git update-ref -d HEAD
            (rp / "file2").write_text(TEST_CONTENT)
            repo.commit("Add file2")
            repo.merge([c1])
            (rp / "file3").write_text(TEST_CONTENT)
            repo.commit("add file3")
            obj_id_hex = repo.repo.refs[b"HEAD"].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=obj_id)
            loader = git_loader(str(rp))
            loader.load()
        return (loader, swhid)

    def check_revision_two_roots(self, ert, p, swhid):
        assert ert.repo.refs[b"HEAD"].decode() == swhid.object_id.hex()

        (c3,) = ert.repo[hashutil.hash_to_bytehex(swhid.object_id)].parents
        assert len(ert.repo[c3].parents) == 2

    def load_repo_two_heads(self, git_loader):
        #
        #    1---2----4      <-- master and b1
        #         \
        #          ----3     <-- b2
        #
        repo = _TestRepo()
        with repo as rp:
            (rp / "file1").write_text(TEST_CONTENT)
            repo.commit("Add file1")

            (rp / "file2").write_text(TEST_CONTENT)
            c2 = repo.commit("Add file2")

            repo.repo.refs[b"refs/heads/b2"] = c2  # branch b2 from master

            (rp / "file3").write_text(TEST_CONTENT)
            repo.commit("add file3", ref=b"refs/heads/b2")

            (rp / "file4").write_text(TEST_CONTENT)
            c4 = repo.commit("add file4", ref=b"refs/heads/master")
            repo.repo.refs[b"refs/heads/b1"] = c4  # branch b1 from master

            obj_id_hex = repo.repo.refs[b"HEAD"].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=obj_id)
            loader = git_loader(str(rp))
            loader.load()
        return (loader, swhid)

    def check_snapshot_two_heads(self, ert, p, swhid):
        assert (
            hashutil.hash_to_bytehex(swhid.object_id)
            == ert.repo.refs[b"HEAD"]
            == ert.repo.refs[b"refs/heads/master"]
            == ert.repo.refs[b"refs/remotes/origin/HEAD"]
            == ert.repo.refs[b"refs/remotes/origin/master"]
            == ert.repo.refs[b"refs/remotes/origin/b1"]
        )

        c4_id = hashutil.hash_to_bytehex(swhid.object_id)
        c3_id = ert.repo.refs[b"refs/remotes/origin/b2"]

        assert ert.repo[c3_id].parents == ert.repo[c4_id].parents

    def load_repo_two_double_fork_merge(self, git_loader):
        #
        #     2---4---6
        #    /   /   /
        #   1---3---5
        #
        repo = _TestRepo()
        with repo as rp:
            (rp / "file1").write_text(TEST_CONTENT)
            c1 = repo.commit("Add file1")  # create commit 1
            repo.repo.refs[b"refs/heads/c1"] = c1  # branch c1 from master

            (rp / "file2").write_text(TEST_CONTENT)
            repo.commit("Add file2")  # create commit 2

            (rp / "file3").write_text(TEST_CONTENT)
            c3 = repo.commit("Add file3", ref=b"refs/heads/c1")  # create commit 3 on c1
            repo.repo.refs[b"refs/heads/c3"] = c3  # branch c3 from c1

            repo.merge([c3])  # create commit 4

            (rp / "file5").write_text(TEST_CONTENT)
            c5 = repo.commit("Add file3", ref=b"refs/heads/c3")  # create commit 5 on c3

            repo.merge([c5])  # create commit 6

            obj_id_hex = repo.repo.refs[b"HEAD"].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=obj_id)
            loader = git_loader(str(rp))
            loader.load()
        return (loader, swhid)

    def check_revision_two_double_fork_merge(self, ert, p, swhid):
        assert ert.repo.refs[b"HEAD"].decode() == swhid.object_id.hex()

    def check_snapshot_two_double_fork_merge(self, ert, p, swhid):
        assert (
            hashutil.hash_to_bytehex(swhid.object_id)
            == ert.repo.refs[b"HEAD"]
            == ert.repo.refs[b"refs/heads/master"]
            == ert.repo.refs[b"refs/remotes/origin/HEAD"]
            == ert.repo.refs[b"refs/remotes/origin/master"]
        )

        (c4_id, c5_id) = ert.repo[swhid.object_id.hex().encode()].parents
        assert c5_id == ert.repo.refs[b"refs/remotes/origin/c3"]

        (c2_id, c3_id) = ert.repo[c4_id].parents
        assert c3_id == ert.repo.refs[b"refs/remotes/origin/c1"]

    def load_repo_triple_merge(self, git_loader):
        #
        #       .---.---5
        #      /   /   /
        #     2   3   4
        #    /   /   /
        #   1---.---.
        #
        repo = _TestRepo()
        with repo as rp:
            (rp / "file1").write_text(TEST_CONTENT)
            c1 = repo.commit("Commit 1")
            repo.repo.refs[b"refs/heads/b1"] = c1
            repo.repo.refs[b"refs/heads/b2"] = c1

            repo.commit("Commit 2")
            c3 = repo.commit("Commit 3", ref=b"refs/heads/b1")
            c4 = repo.commit("Commit 4", ref=b"refs/heads/b2")
            repo.merge([c3, c4])

            obj_id_hex = repo.repo.refs[b"HEAD"].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=obj_id)
            loader = git_loader(str(rp))
            loader.load()
        return (loader, swhid)

    def check_revision_triple_merge(self, ert, p, swhid):
        assert ert.repo.refs[b"HEAD"].decode() == swhid.object_id.hex()

    def check_snapshot_triple_merge(self, ert, p, swhid):
        assert (
            hashutil.hash_to_bytehex(swhid.object_id)
            == ert.repo.refs[b"HEAD"]
            == ert.repo.refs[b"refs/heads/master"]
            == ert.repo.refs[b"refs/remotes/origin/HEAD"]
            == ert.repo.refs[b"refs/remotes/origin/master"]
        )

        (c2_id, c3_id, c4_id) = ert.repo[swhid.object_id.hex().encode()].parents
        assert c3_id == ert.repo.refs[b"refs/remotes/origin/b1"]
        assert c4_id == ert.repo.refs[b"refs/remotes/origin/b2"]

        assert (
            ert.repo[c2_id].parents
            == ert.repo[c3_id].parents
            == ert.repo[c4_id].parents
        )

    def load_repo_filtered_objects(self, git_loader):
        repo = _TestRepo()
        with repo as rp:
            file_1, id_1 = hash_content(b"test1")
            file_2, id_2 = hash_content(b"test2")
            file_3, id_3 = hash_content(b"test3")

            (rp / "file").write_bytes(file_1)
            (rp / "hidden_file").write_bytes(file_2)
            (rp / "absent_file").write_bytes(file_3)

            repo.commit()
            obj_id_hex = repo.repo.refs[b"HEAD"].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=obj_id)
            loader = git_loader(str(rp))
            loader.load()

        # alter the content of the storage
        # 1/ make file 2 an hidden file object
        loader.storage._allow_overwrite = True
        cnt2 = attr.evolve(
            loader.storage.content_get([id_2])[0], status="hidden", data=file_2
        )
        loader.storage.content_add([cnt2])
        assert loader.storage.content_get([id_2])[0].status == "hidden"

        # 2/ make file 3 an skipped file object
        cnt3 = loader.storage.content_get([id_3])[0].to_dict()
        cnt3["status"] = "absent"
        cnt3["reason"] = "no reason"
        sk_cnt3 = SkippedContent.from_dict(cnt3)
        loader.storage.skipped_content_add([sk_cnt3])
        # dirty dirty dirty... let's pretend it is the equivalent of writing sql
        # queries in the postgresql backend
        for hashkey in loader.storage._cql_runner._content_indexes:
            loader.storage._cql_runner._content_indexes[hashkey].pop(cnt3[hashkey])

        return (loader, swhid)

    def check_revision_filtered_objects(self, ert, p, swhid):
        ert.checkout(b"HEAD")
        assert (p / "file").read_bytes() == b"test1"
        assert (p / "hidden_file").read_bytes() == HIDDEN_MESSAGE
        assert (p / "absent_file").read_bytes() == SKIPPED_MESSAGE

    def load_repo_null_fields(self, git_loader):
        # Our schema doesn't enforce a lot of non-null revision fields. We need
        # to check these cases don't break the cooker.
        repo = _TestRepo()
        with repo as rp:
            (rp / "file").write_text(TEST_CONTENT)
            c = repo.commit("initial commit")
            loader = git_loader(str(rp))
            loader.load()
            repo.repo.refs[b"HEAD"].decode()
            dir_id_hex = repo.repo[c].tree.decode()
            dir_id = hashutil.hash_to_bytes(dir_id_hex)

        test_revision = Revision(
            message=b"",
            author=Person(name=None, email=None, fullname=b""),
            date=None,
            committer=Person(name=None, email=None, fullname=b""),
            committer_date=None,
            parents=(),
            type=RevisionType.GIT,
            directory=dir_id,
            metadata={},
            synthetic=True,
        )

        storage = loader.storage
        storage.revision_add([test_revision])
        return (loader, test_revision.swhid())

    def check_revision_null_fields(self, ert, p, swhid):
        ert.checkout(b"HEAD")
        assert (p / "file").stat().st_mode == 0o100644

    def load_repo_tags(self, git_loader):
        #        v-- t2
        #
        #    1---2----5      <-- master, t5, and t5a (annotated)
        #         \
        #          ----3----4     <-- t4a (annotated)
        #
        repo = _TestRepo()
        with repo as rp:
            (rp / "file1").write_text(TEST_CONTENT)
            repo.commit("Add file1")

            (rp / "file2").write_text(TEST_CONTENT)
            repo.commit("Add file2")  # create c2

            repo.tag(b"t2")

            (rp / "file3").write_text(TEST_CONTENT)
            repo.commit("add file3")

            (rp / "file4").write_text(TEST_CONTENT)
            repo.commit("add file4")

            repo.tag(b"t4a", message=b"tag 4")

            # Go back to c2
            repo.git_shell("reset", "--hard", "HEAD^^")

            (rp / "file5").write_text(TEST_CONTENT)
            repo.commit("add file5")  # create c5

            repo.tag(b"t5")
            repo.tag(b"t5a", message=b"tag 5")

            obj_id_hex = repo.repo.refs[b"HEAD"].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            swhid = CoreSWHID(object_type=ObjectType.REVISION, object_id=obj_id)
            loader = git_loader(str(rp))
            loader.load()
        return (loader, swhid)

    def check_snapshot_tags(self, ert, p, swhid):
        assert (
            hashutil.hash_to_bytehex(swhid.object_id)
            == ert.repo.refs[b"HEAD"]
            == ert.repo.refs[b"refs/heads/master"]
            == ert.repo.refs[b"refs/remotes/origin/HEAD"]
            == ert.repo.refs[b"refs/remotes/origin/master"]
            == ert.repo.refs[b"refs/tags/t5"]
        )

        c2_id = ert.repo.refs[b"refs/tags/t2"]
        c5_id = hashutil.hash_to_bytehex(swhid.object_id)

        assert ert.repo[c5_id].parents == [c2_id]

        t5a = ert.repo[ert.repo.refs[b"refs/tags/t5a"]]
        # TODO: investigate why new dulwich adds \n
        assert t5a.message in (b"tag 5", b"tag 5\n")
        assert t5a.object == (dulwich.objects.Commit, c5_id)

        t4a = ert.repo[ert.repo.refs[b"refs/tags/t4a"]]
        (_, c4_id) = t4a.object
        assert ert.repo[c4_id].message == b"add file4\n"  # TODO: ditto
        (c3_id,) = ert.repo[c4_id].parents
        assert ert.repo[c3_id].message == b"add file3\n"  # TODO: ditto
        assert ert.repo[c3_id].parents == [c2_id]


class TestRevisionCooker(RepoFixtures):
    def test_revision_simple(self, git_loader, cook_extract_revision):
        (loader, swhid) = self.load_repo_simple(git_loader)
        with cook_extract_revision(loader.storage, swhid) as (ert, p):
            self.check_revision_simple(ert, p, swhid)

    def test_revision_two_roots(self, git_loader, cook_extract_revision):
        (loader, swhid) = self.load_repo_two_roots(git_loader)
        with cook_extract_revision(loader.storage, swhid) as (ert, p):
            self.check_revision_two_roots(ert, p, swhid)

    def test_revision_two_double_fork_merge(self, git_loader, cook_extract_revision):
        (loader, swhid) = self.load_repo_two_double_fork_merge(git_loader)
        with cook_extract_revision(loader.storage, swhid) as (ert, p):
            self.check_revision_two_double_fork_merge(ert, p, swhid)

    def test_revision_triple_merge(self, git_loader, cook_extract_revision):
        (loader, swhid) = self.load_repo_triple_merge(git_loader)
        with cook_extract_revision(loader.storage, swhid) as (ert, p):
            self.check_revision_triple_merge(ert, p, swhid)

    def test_revision_filtered_objects(self, git_loader, cook_extract_revision):
        (loader, swhid) = self.load_repo_filtered_objects(git_loader)
        with cook_extract_revision(loader.storage, swhid) as (ert, p):
            self.check_revision_filtered_objects(ert, p, swhid)

    def test_revision_null_fields(self, git_loader, cook_extract_revision):
        (loader, swhid) = self.load_repo_null_fields(git_loader)
        with cook_extract_revision(loader.storage, swhid, fsck=False) as (ert, p):
            self.check_revision_null_fields(ert, p, swhid)

    @pytest.mark.parametrize("ingest_target_revision", [False, True])
    def test_revision_submodule(
        self, swh_storage, cook_extract_revision, ingest_target_revision
    ):
        date = TimestampWithTimezone.from_datetime(
            datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
        )

        target_rev = Revision(
            message=b"target_rev",
            author=Person.from_fullname(b"me <test@example.org>"),
            date=date,
            committer=Person.from_fullname(b"me <test@example.org>"),
            committer_date=date,
            parents=(),
            type=RevisionType.GIT,
            directory=bytes.fromhex("3333333333333333333333333333333333333333"),
            metadata={},
            synthetic=True,
        )
        if ingest_target_revision:
            swh_storage.revision_add([target_rev])

        dir = Directory(
            entries=(
                DirectoryEntry(
                    name=b"submodule",
                    type="rev",
                    target=target_rev.id,
                    perms=0o160000,
                ),
            ),
        )
        swh_storage.directory_add([dir])

        rev = Revision(
            message=b"msg",
            author=Person.from_fullname(b"me <test@example.org>"),
            date=date,
            committer=Person.from_fullname(b"me <test@example.org>"),
            committer_date=date,
            parents=(),
            type=RevisionType.GIT,
            directory=dir.id,
            metadata={},
            synthetic=True,
        )
        swh_storage.revision_add([rev])

        with cook_extract_revision(swh_storage, rev.swhid()) as (ert, p):
            ert.checkout(b"HEAD")
            pattern = b"160000 submodule\x00%s" % target_rev.id
            tree = ert.repo[b"HEAD"].tree
            assert pattern in ert.repo[tree].as_raw_string()


class TestSnapshotCooker(RepoFixtures):
    def test_snapshot_simple(self, git_loader, cook_extract_snapshot):
        (loader, main_rev_id) = self.load_repo_simple(git_loader)
        snp_id = loader.loaded_snapshot_id
        swhid = CoreSWHID(object_type=ObjectType.SNAPSHOT, object_id=snp_id)
        with cook_extract_snapshot(loader.storage, swhid) as (ert, p):
            self.check_revision_simple(ert, p, main_rev_id)

    def test_snapshot_two_roots(self, git_loader, cook_extract_snapshot):
        (loader, main_rev_id) = self.load_repo_two_roots(git_loader)
        snp_id = loader.loaded_snapshot_id
        swhid = CoreSWHID(object_type=ObjectType.SNAPSHOT, object_id=snp_id)
        with cook_extract_snapshot(loader.storage, swhid) as (ert, p):
            self.check_revision_two_roots(ert, p, main_rev_id)

    def test_snapshot_two_heads(self, git_loader, cook_extract_snapshot):
        (loader, main_rev_id) = self.load_repo_two_heads(git_loader)
        snp_id = loader.loaded_snapshot_id
        swhid = CoreSWHID(object_type=ObjectType.SNAPSHOT, object_id=snp_id)
        with cook_extract_snapshot(loader.storage, swhid) as (ert, p):
            self.check_snapshot_two_heads(ert, p, main_rev_id)

    def test_snapshot_two_double_fork_merge(self, git_loader, cook_extract_snapshot):
        (loader, main_rev_id) = self.load_repo_two_double_fork_merge(git_loader)
        snp_id = loader.loaded_snapshot_id
        swhid = CoreSWHID(object_type=ObjectType.SNAPSHOT, object_id=snp_id)
        with cook_extract_snapshot(loader.storage, swhid) as (ert, p):
            self.check_revision_two_double_fork_merge(ert, p, main_rev_id)
            self.check_snapshot_two_double_fork_merge(ert, p, main_rev_id)

    def test_snapshot_triple_merge(self, git_loader, cook_extract_snapshot):
        (loader, main_rev_id) = self.load_repo_triple_merge(git_loader)
        snp_id = loader.loaded_snapshot_id
        swhid = CoreSWHID(object_type=ObjectType.SNAPSHOT, object_id=snp_id)
        with cook_extract_snapshot(loader.storage, swhid) as (ert, p):
            self.check_revision_triple_merge(ert, p, main_rev_id)
            self.check_snapshot_triple_merge(ert, p, main_rev_id)

    def test_snapshot_filtered_objects(self, git_loader, cook_extract_snapshot):
        (loader, main_rev_id) = self.load_repo_filtered_objects(git_loader)
        snp_id = loader.loaded_snapshot_id
        swhid = CoreSWHID(object_type=ObjectType.SNAPSHOT, object_id=snp_id)
        with cook_extract_snapshot(loader.storage, swhid) as (ert, p):
            self.check_revision_filtered_objects(ert, p, main_rev_id)

    def test_snapshot_tags(self, git_loader, cook_extract_snapshot):
        (loader, main_rev_id) = self.load_repo_tags(git_loader)
        snp_id = loader.loaded_snapshot_id
        swhid = CoreSWHID(object_type=ObjectType.SNAPSHOT, object_id=snp_id)
        with cook_extract_snapshot(loader.storage, swhid) as (ert, p):
            self.check_snapshot_tags(ert, p, main_rev_id)

    def test_original_malformed_objects(self, swh_storage, cook_extract_snapshot):
        """Tests that objects that were originally malformed:

        * are still interpreted somewhat correctly (if the loader could make sense of
          them), especially that they still have links to children
        * have their original manifest in the bundle
        """
        date = TimestampWithTimezone.from_numeric_offset(
            Timestamp(1643819927, 0), 0, False
        )

        content = Content.from_data(b"foo")
        swh_storage.content_add([content])

        # disordered
        # fmt: off
        malformed_dir_manifest = (
            b""
            + b"100644 file2\x00" + content.sha1_git
            + b"100644 file1\x00" + content.sha1_git
        )
        # fmt: on
        directory = Directory(
            entries=(
                DirectoryEntry(
                    name=b"file1", type="file", perms=0o100644, target=content.sha1_git
                ),
                DirectoryEntry(
                    name=b"file2", type="file", perms=0o100644, target=content.sha1_git
                ),
            ),
            raw_manifest=f"tree {len(malformed_dir_manifest)}\x00".encode()
            + malformed_dir_manifest,
        )
        swh_storage.directory_add([directory])

        # 'committer' and 'author' swapped
        # fmt: off
        malformed_rev_manifest = (
            b"tree " + hashutil.hash_to_bytehex(directory.id) + b"\n"
            + b"committer me <test@example.org> 1643819927 +0000\n"
            + b"author me <test@example.org> 1643819927 +0000\n"
            + b"\n"
            + b"rev"
        )
        # fmt: on
        revision = Revision(
            message=b"rev",
            author=Person.from_fullname(b"me <test@example.org>"),
            date=date,
            committer=Person.from_fullname(b"me <test@example.org>"),
            committer_date=date,
            parents=(),
            type=RevisionType.GIT,
            directory=directory.id,
            synthetic=True,
            raw_manifest=f"commit {len(malformed_rev_manifest)}\x00".encode()
            + malformed_rev_manifest,
        )
        swh_storage.revision_add([revision])

        # 'tag' and 'tagger' swapped
        # fmt: off
        malformed_rel_manifest = (
            b"object " + hashutil.hash_to_bytehex(revision.id) + b"\n"
            + b"type commit\n"
            + b"tagger me <test@example.org> 1643819927 +0000\n"
            + b"tag v1.1.0\n"
        )
        # fmt: on

        release = Release(
            name=b"v1.1.0",
            message=None,
            author=Person.from_fullname(b"me <test@example.org>"),
            date=date,
            target=revision.id,
            target_type=ReleaseTargetType.REVISION,
            synthetic=True,
            raw_manifest=f"tag {len(malformed_rel_manifest)}\x00".encode()
            + malformed_rel_manifest,
        )
        swh_storage.release_add([release])

        snapshot = Snapshot(
            branches={
                b"refs/tags/v1.1.0": SnapshotBranch(
                    target=release.id, target_type=SnapshotTargetType.RELEASE
                ),
                b"HEAD": SnapshotBranch(
                    target=revision.id, target_type=SnapshotTargetType.REVISION
                ),
            }
        )
        swh_storage.snapshot_add([snapshot])

        with cook_extract_snapshot(swh_storage, snapshot.swhid()) as (ert, p):
            tag = ert.repo[b"refs/tags/v1.1.0"]
            assert tag.as_raw_string() == malformed_rel_manifest

            commit = ert.repo[tag.object[1]]
            assert commit.as_raw_string() == malformed_rev_manifest

            tree = ert.repo[commit.tree]
            assert tree.as_raw_string() == malformed_dir_manifest
