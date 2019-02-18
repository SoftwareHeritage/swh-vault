# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import datetime
import gzip
import io
import os
import pathlib
import pytest
import subprocess
import tarfile
import tempfile
import unittest
import unittest.mock

import dulwich.fastexport
import dulwich.index
import dulwich.objects
import dulwich.porcelain
import dulwich.repo

from swh.loader.git.from_disk import GitLoaderFromDisk
from swh.model import hashutil
from swh.model.from_disk import Directory
from swh.vault.cookers import DirectoryCooker, RevisionGitfastCooker
from swh.vault.tests.vault_testing import hash_content
from swh.vault.to_disk import SKIPPED_MESSAGE, HIDDEN_MESSAGE


class TestRepo:
    """A tiny context manager for a test git repository, with some utility
    functions to perform basic git stuff.
    """
    def __enter__(self):
        self.tmp_dir = tempfile.TemporaryDirectory(prefix='tmp-vault-repo-')
        self.repo_dir = self.tmp_dir.__enter__()
        self.repo = dulwich.repo.Repo.init(self.repo_dir)
        self.author_name = b'Test Author'
        self.author_email = b'test@softwareheritage.org'
        self.author = b'%s <%s>' % (self.author_name, self.author_email)
        self.base_date = 258244200
        self.counter = 0
        return pathlib.Path(self.repo_dir)

    def __exit__(self, exc, value, tb):
        self.tmp_dir.__exit__(exc, value, tb)

    def checkout(self, rev_sha):
        rev = self.repo[rev_sha]
        dulwich.index.build_index_from_tree(self.repo_dir,
                                            self.repo.index_path(),
                                            self.repo.object_store,
                                            rev.tree)

    def git_shell(self, *cmd, stdout=subprocess.DEVNULL, **kwargs):
        name = self.author_name
        email = self.author_email
        date = '%d +0000' % (self.base_date + self.counter)
        env = {
            # Set git commit format
            'GIT_AUTHOR_NAME': name,
            'GIT_AUTHOR_EMAIL': email,
            'GIT_AUTHOR_DATE': date,
            'GIT_COMMITTER_NAME': name,
            'GIT_COMMITTER_EMAIL': email,
            'GIT_COMMITTER_DATE': date,
            # Ignore all the system-wide and user configurations
            'GIT_CONFIG_NOSYSTEM': '1',
            'HOME': str(self.tmp_dir),
            'XDG_CONFIG_HOME': str(self.tmp_dir),
        }
        kwargs.setdefault('env', {}).update(env)

        subprocess.check_call(('git', '-C', self.repo_dir) + cmd,
                              stdout=stdout, **kwargs)

    def commit(self, message='Commit test\n', ref=b'HEAD'):
        """Commit the current working tree in a new commit with message on
        the branch 'ref'.

        At the end of the commit, the reference should stay the same
        and the index should be clean.

        """
        self.git_shell('add', '.')
        message = message.encode() + b'\n'
        ret = self.repo.do_commit(
            message=message, committer=self.author,
            commit_timestamp=self.base_date + self.counter,
            commit_timezone=0,
            ref=ref)
        self.counter += 1

        # committing on another branch leaves
        # dangling files in index
        if ref != b'HEAD':
            # XXX this should work (but does not)
            # dulwich.porcelain.reset(self.repo, 'hard')
            self.git_shell('reset', '--hard', 'HEAD')
        return ret

    def merge(self, parent_sha_list, message='Merge branches.'):
        self.git_shell('merge', '--allow-unrelated-histories',
                       '-m', message, *[p.decode() for p in parent_sha_list])
        self.counter += 1
        return self.repo.refs[b'HEAD']

    def print_debug_graph(self, reflog=False):
        args = ['log', '--all', '--graph', '--decorate']
        if reflog:
            args.append('--reflog')
        self.git_shell(*args, stdout=None)


@pytest.fixture
def swh_git_loader(swh_vault):
    loader = GitLoaderFromDisk()
    loader.storage = swh_vault.storage
    return loader


def load(loader, repo_path):
    """Load a repository in the test storage"""
    loader.load('fake_origin', repo_path, datetime.datetime.now())


@contextlib.contextmanager
def cook_extract_directory(storage, obj_id):
    """Context manager that cooks a directory and extract it."""
    backend = unittest.mock.MagicMock()
    backend.storage = storage
    cooker = DirectoryCooker(
        'directory', obj_id, backend=backend, storage=storage)
    cooker.fileobj = io.BytesIO()
    assert cooker.check_exists()
    cooker.prepare_bundle()
    cooker.fileobj.seek(0)
    with tempfile.TemporaryDirectory(prefix='tmp-vault-extract-') as td:
        with tarfile.open(fileobj=cooker.fileobj, mode='r') as tar:
            tar.extractall(td)
        yield pathlib.Path(td) / hashutil.hash_to_hex(obj_id)
    cooker.storage = None


@contextlib.contextmanager
def cook_stream_revision_gitfast(storage, obj_id):
    """Context manager that cooks a revision and stream its fastexport."""
    backend = unittest.mock.MagicMock()
    backend.storage = storage
    cooker = RevisionGitfastCooker(
        'revision_gitfast', obj_id, backend=backend, storage=storage)
    cooker.fileobj = io.BytesIO()
    assert cooker.check_exists()
    cooker.prepare_bundle()
    cooker.fileobj.seek(0)
    fastexport_stream = gzip.GzipFile(fileobj=cooker.fileobj)
    yield fastexport_stream
    cooker.storage = None


@contextlib.contextmanager
def cook_extract_revision_gitfast(storage, obj_id):
    """Context manager that cooks a revision and extract it."""
    test_repo = TestRepo()
    with cook_stream_revision_gitfast(storage, obj_id) as stream, \
            test_repo as p:
        processor = dulwich.fastexport.GitImportProcessor(test_repo.repo)
        processor.import_stream(stream)
        yield test_repo, p


TEST_CONTENT = ("   test content\n"
                "and unicode \N{BLACK HEART SUIT}\n"
                " and trailing spaces   ")
TEST_EXECUTABLE = b'\x42\x40\x00\x00\x05'


class TestDirectoryCooker:
    def test_directory_simple(self, swh_git_loader):
        repo = TestRepo()
        with repo as rp:
            (rp / 'file').write_text(TEST_CONTENT)
            (rp / 'executable').write_bytes(TEST_EXECUTABLE)
            (rp / 'executable').chmod(0o755)
            (rp / 'link').symlink_to('file')
            (rp / 'dir1/dir2').mkdir(parents=True)
            (rp / 'dir1/dir2/file').write_text(TEST_CONTENT)
            c = repo.commit()
            load(swh_git_loader, str(rp))

            obj_id_hex = repo.repo[c].tree.decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)

        with cook_extract_directory(swh_git_loader.storage, obj_id) as p:
            assert (p / 'file').stat().st_mode == 0o100644
            assert (p / 'file').read_text() == TEST_CONTENT
            assert (p / 'executable').stat().st_mode == 0o100755
            assert (p / 'executable').read_bytes() == TEST_EXECUTABLE
            assert (p / 'link').is_symlink
            assert os.readlink(str(p / 'link')) == 'file'
            assert (p / 'dir1/dir2/file').stat().st_mode == 0o100644
            assert (p / 'dir1/dir2/file').read_text() == TEST_CONTENT

            directory = Directory.from_disk(path=bytes(p))
            assert obj_id_hex == hashutil.hash_to_hex(directory.hash)

    def test_directory_filtered_objects(self, swh_git_loader):
        repo = TestRepo()
        with repo as rp:
            file_1, id_1 = hash_content(b'test1')
            file_2, id_2 = hash_content(b'test2')
            file_3, id_3 = hash_content(b'test3')

            (rp / 'file').write_bytes(file_1)
            (rp / 'hidden_file').write_bytes(file_2)
            (rp / 'absent_file').write_bytes(file_3)

            c = repo.commit()
            load(swh_git_loader, str(rp))

            obj_id_hex = repo.repo[c].tree.decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)

        # FIXME: storage.content_update() should be changed to allow things
        # like that
        with swh_git_loader.storage.get_db().transaction() as cur:
            cur.execute("""update content set status = 'visible'
                           where sha1 = %s""", (id_1,))
            cur.execute("""update content set status = 'hidden'
                           where sha1 = %s""", (id_2,))
            cur.execute("""update content set status = 'absent'
                           where sha1 = %s""", (id_3,))

        with cook_extract_directory(swh_git_loader.storage, obj_id) as p:
            assert (p / 'file').read_bytes() == b'test1'
            assert (p / 'hidden_file').read_bytes() == HIDDEN_MESSAGE
            assert (p / 'absent_file').read_bytes() == SKIPPED_MESSAGE

    def test_directory_bogus_perms(self, swh_git_loader):
        # Some early git repositories have 664/775 permissions... let's check
        # if all the weird modes are properly normalized in the directory
        # cooker.
        repo = TestRepo()
        with repo as rp:
            (rp / 'file').write_text(TEST_CONTENT)
            (rp / 'file').chmod(0o664)
            (rp / 'executable').write_bytes(TEST_EXECUTABLE)
            (rp / 'executable').chmod(0o775)
            (rp / 'wat').write_text(TEST_CONTENT)
            (rp / 'wat').chmod(0o604)
            c = repo.commit()
            load(swh_git_loader, str(rp))

            obj_id_hex = repo.repo[c].tree.decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)

        with cook_extract_directory(swh_git_loader.storage, obj_id) as p:
            assert (p / 'file').stat().st_mode == 0o100644
            assert (p / 'executable').stat().st_mode == 0o100755
            assert (p / 'wat').stat().st_mode == 0o100644

    def test_directory_revision_data(self, swh_git_loader):
        target_rev = '0e8a3ad980ec179856012b7eecf4327e99cd44cd'
        d = hashutil.hash_to_bytes('17a3e48bce37be5226490e750202ad3a9a1a3fe9')

        dir = {
            'id': d,
            'entries': [
                {
                    'name': b'submodule',
                    'type': 'rev',
                    'target': hashutil.hash_to_bytes(target_rev),
                    'perms': 0o100644,
                }
            ],
        }
        swh_git_loader.storage.directory_add([dir])

        with cook_extract_directory(swh_git_loader.storage, d) as p:
            assert (p / 'submodule').is_symlink()
            assert os.readlink(str(p / 'submodule')) == target_rev


class TestRevisionGitfastCooker:
    def test_revision_simple(self, swh_git_loader):
        #
        #     1--2--3--4--5--6--7
        #
        storage = swh_git_loader.storage
        repo = TestRepo()
        with repo as rp:
            (rp / 'file1').write_text(TEST_CONTENT)
            repo.commit('add file1')
            (rp / 'file2').write_text(TEST_CONTENT)
            repo.commit('add file2')
            (rp / 'dir1/dir2').mkdir(parents=True)
            (rp / 'dir1/dir2/file').write_text(TEST_CONTENT)
            repo.commit('add dir1/dir2/file')
            (rp / 'bin1').write_bytes(TEST_EXECUTABLE)
            (rp / 'bin1').chmod(0o755)
            repo.commit('add bin1')
            (rp / 'link1').symlink_to('file1')
            repo.commit('link link1 to file1')
            (rp / 'file2').unlink()
            repo.commit('remove file2')
            (rp / 'bin1').rename(rp / 'bin')
            repo.commit('rename bin1 to bin')
            load(swh_git_loader, str(rp))
            obj_id_hex = repo.repo.refs[b'HEAD'].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)

        with cook_extract_revision_gitfast(storage, obj_id) as (ert, p):
            ert.checkout(b'HEAD')
            assert (p / 'file1').stat().st_mode == 0o100644
            assert (p / 'file1').read_text() == TEST_CONTENT
            assert (p / 'link1').is_symlink
            assert os.readlink(str(p / 'link1')) == 'file1'
            assert (p / 'bin').stat().st_mode == 0o100755
            assert (p / 'bin').read_bytes() == TEST_EXECUTABLE
            assert (p / 'dir1/dir2/file').read_text() == TEST_CONTENT
            assert (p / 'dir1/dir2/file').stat().st_mode == 0o100644
            assert ert.repo.refs[b'HEAD'].decode() == obj_id_hex

    def test_revision_two_roots(self, swh_git_loader):
        #
        #    1----3---4
        #        /
        #   2----
        #
        storage = swh_git_loader.storage
        repo = TestRepo()
        with repo as rp:
            (rp / 'file1').write_text(TEST_CONTENT)
            c1 = repo.commit('Add file1')
            del repo.repo.refs[b'refs/heads/master']  # git update-ref -d HEAD
            (rp / 'file2').write_text(TEST_CONTENT)
            repo.commit('Add file2')
            repo.merge([c1])
            (rp / 'file3').write_text(TEST_CONTENT)
            repo.commit('add file3')
            obj_id_hex = repo.repo.refs[b'HEAD'].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            load(swh_git_loader, str(rp))

        with cook_extract_revision_gitfast(storage, obj_id) as (ert, p):
            assert ert.repo.refs[b'HEAD'].decode() == obj_id_hex

    def test_revision_two_double_fork_merge(self, swh_git_loader):
        #
        #     2---4---6
        #    /   /   /
        #   1---3---5
        #
        storage = swh_git_loader.storage
        repo = TestRepo()
        with repo as rp:
            (rp / 'file1').write_text(TEST_CONTENT)
            c1 = repo.commit('Add file1')
            repo.repo.refs[b'refs/heads/c1'] = c1

            (rp / 'file2').write_text(TEST_CONTENT)
            repo.commit('Add file2')

            (rp / 'file3').write_text(TEST_CONTENT)
            c3 = repo.commit('Add file3', ref=b'refs/heads/c1')
            repo.repo.refs[b'refs/heads/c3'] = c3

            repo.merge([c3])

            (rp / 'file5').write_text(TEST_CONTENT)
            c5 = repo.commit('Add file3', ref=b'refs/heads/c3')

            repo.merge([c5])

            obj_id_hex = repo.repo.refs[b'HEAD'].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            load(swh_git_loader, str(rp))

        with cook_extract_revision_gitfast(storage, obj_id) as (ert, p):
            assert ert.repo.refs[b'HEAD'].decode() == obj_id_hex

    def test_revision_triple_merge(self, swh_git_loader):
        #
        #       .---.---5
        #      /   /   /
        #     2   3   4
        #    /   /   /
        #   1---.---.
        #
        storage = swh_git_loader.storage
        repo = TestRepo()
        with repo as rp:
            (rp / 'file1').write_text(TEST_CONTENT)
            c1 = repo.commit('Commit 1')
            repo.repo.refs[b'refs/heads/b1'] = c1
            repo.repo.refs[b'refs/heads/b2'] = c1

            repo.commit('Commit 2')
            c3 = repo.commit('Commit 3', ref=b'refs/heads/b1')
            c4 = repo.commit('Commit 4', ref=b'refs/heads/b2')
            repo.merge([c3, c4])

            obj_id_hex = repo.repo.refs[b'HEAD'].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            load(swh_git_loader, str(rp))

        with cook_extract_revision_gitfast(storage, obj_id) as (ert, p):
            assert ert.repo.refs[b'HEAD'].decode() == obj_id_hex

    def test_revision_filtered_objects(self, swh_git_loader):
        storage = swh_git_loader.storage
        repo = TestRepo()
        with repo as rp:
            file_1, id_1 = hash_content(b'test1')
            file_2, id_2 = hash_content(b'test2')
            file_3, id_3 = hash_content(b'test3')

            (rp / 'file').write_bytes(file_1)
            (rp / 'hidden_file').write_bytes(file_2)
            (rp / 'absent_file').write_bytes(file_3)

            repo.commit()
            obj_id_hex = repo.repo.refs[b'HEAD'].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)
            load(swh_git_loader, str(rp))

        # FIXME: storage.content_update() should be changed to allow things
        # like that
        with storage.get_db().transaction() as cur:
            cur.execute("""update content set status = 'visible'
                           where sha1 = %s""", (id_1,))
            cur.execute("""update content set status = 'hidden'
                           where sha1 = %s""", (id_2,))
            cur.execute("""update content set status = 'absent'
                           where sha1 = %s""", (id_3,))

        with cook_extract_revision_gitfast(storage, obj_id) as (ert, p):
            ert.checkout(b'HEAD')
            assert (p / 'file').read_bytes() == b'test1'
            assert (p / 'hidden_file').read_bytes() == HIDDEN_MESSAGE
            assert (p / 'absent_file').read_bytes() == SKIPPED_MESSAGE

    def test_revision_bogus_perms(self, swh_git_loader):
        # Some early git repositories have 664/775 permissions... let's check
        # if all the weird modes are properly normalized in the revision
        # cooker.
        storage = swh_git_loader.storage
        repo = TestRepo()
        with repo as rp:
            (rp / 'file').write_text(TEST_CONTENT)
            (rp / 'file').chmod(0o664)
            (rp / 'executable').write_bytes(TEST_EXECUTABLE)
            (rp / 'executable').chmod(0o775)
            (rp / 'wat').write_text(TEST_CONTENT)
            (rp / 'wat').chmod(0o604)
            repo.commit('initial commit')
            load(swh_git_loader, str(rp))
            obj_id_hex = repo.repo.refs[b'HEAD'].decode()
            obj_id = hashutil.hash_to_bytes(obj_id_hex)

        with cook_extract_revision_gitfast(storage, obj_id) as (ert, p):
            ert.checkout(b'HEAD')
            assert (p / 'file').stat().st_mode == 0o100644
            assert (p / 'executable').stat().st_mode == 0o100755
            assert (p / 'wat').stat().st_mode == 0o100644

    def test_revision_null_fields(self, swh_git_loader):
        # Our schema doesn't enforce a lot of non-null revision fields. We need
        # to check these cases don't break the cooker.
        storage = swh_git_loader.storage
        repo = TestRepo()
        with repo as rp:
            (rp / 'file').write_text(TEST_CONTENT)
            c = repo.commit('initial commit')
            load(swh_git_loader, str(rp))
            repo.repo.refs[b'HEAD'].decode()
            dir_id_hex = repo.repo[c].tree.decode()
            dir_id = hashutil.hash_to_bytes(dir_id_hex)

        test_id = b'56789012345678901234'
        test_revision = {
            'id': test_id,
            'message': None,
            'author': {'name': None, 'email': None, 'fullname': ''},
            'date': None,
            'committer': {'name': None, 'email': None, 'fullname': ''},
            'committer_date': None,
            'parents': [],
            'type': 'git',
            'directory': dir_id,
            'metadata': {},
            'synthetic': True
        }

        storage.revision_add([test_revision])

        with cook_extract_revision_gitfast(storage, test_id) as (ert, p):
            ert.checkout(b'HEAD')
            assert (p / 'file').stat().st_mode == 0o100644

    def test_revision_revision_data(self, swh_git_loader):
        storage = swh_git_loader.storage
        target_rev = '0e8a3ad980ec179856012b7eecf4327e99cd44cd'
        d = hashutil.hash_to_bytes('17a3e48bce37be5226490e750202ad3a9a1a3fe9')
        r = hashutil.hash_to_bytes('1ecc9270c4fc61cfddbc65a774e91ef5c425a6f0')

        dir = {
            'id': d,
            'entries': [
                {
                    'name': b'submodule',
                    'type': 'rev',
                    'target': hashutil.hash_to_bytes(target_rev),
                    'perms': 0o100644,
                }
            ],
        }
        storage.directory_add([dir])

        rev = {
            'id': r,
            'message': None,
            'author': {'name': None, 'email': None, 'fullname': ''},
            'date': None,
            'committer': {'name': None, 'email': None, 'fullname': ''},
            'committer_date': None,
            'parents': [],
            'type': 'git',
            'directory': d,
            'metadata': {},
            'synthetic': True
        }
        storage.revision_add([rev])

        with cook_stream_revision_gitfast(storage, r) as stream:
            pattern = 'M 160000 {} submodule'.format(target_rev).encode()
            assert pattern in stream.read()
