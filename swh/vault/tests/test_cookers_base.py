# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pathlib
import tempfile
import unittest
from unittest.mock import MagicMock

from swh.model import hashutil
from swh.vault.cookers.base import (BaseVaultCooker, get_tar_bytes,
                                    BundleTooLargeError)


TEST_BUNDLE_CHUNKS = [b"test content 1\n",
                      b"test content 2\n",
                      b"test content 3\n"]
TEST_BUNDLE_CONTENT = b''.join(TEST_BUNDLE_CHUNKS)
TEST_OBJ_TYPE = 'test_type'
TEST_HEX_ID = '17a3e48bce37be5226490e750202ad3a9a1a3fe9'
TEST_OBJ_ID = hashutil.hash_to_bytes(TEST_HEX_ID)


class BaseVaultCookerMock(BaseVaultCooker):
    CACHE_TYPE_KEY = TEST_OBJ_TYPE

    def __init__(self, *args, **kwargs):
        super().__init__(self.CACHE_TYPE_KEY, TEST_OBJ_ID, *args, **kwargs)
        self.storage = MagicMock()
        self.backend = MagicMock()

    def check_exists(self):
        return True

    def prepare_bundle(self):
        for chunk in TEST_BUNDLE_CHUNKS:
            yield chunk


class TestBaseVaultCooker(unittest.TestCase):
    def test_simple_cook(self):
        cooker = BaseVaultCookerMock()
        cooker.cook()
        cooker.backend.put_bundle.assert_called_once_with(
            TEST_OBJ_TYPE, TEST_OBJ_ID, TEST_BUNDLE_CONTENT)
        cooker.backend.set_status.assert_called_with(
            TEST_OBJ_TYPE, TEST_OBJ_ID, 'done')
        cooker.backend.set_progress.assert_called_with(
            TEST_OBJ_TYPE, TEST_OBJ_ID, None)
        cooker.backend.send_notif.assert_called_with(
            TEST_OBJ_TYPE, TEST_OBJ_ID)

    def test_code_exception_cook(self):
        cooker = BaseVaultCookerMock()
        cooker.prepare_bundle = MagicMock()
        cooker.prepare_bundle.side_effect = RuntimeError("Nope")
        cooker.cook()

        # Potentially remove this when we have objstorage streaming
        cooker.backend.put_bundle.assert_not_called()

        cooker.backend.set_status.assert_called_with(
            TEST_OBJ_TYPE, TEST_OBJ_ID, 'failed')
        self.assertNotIn("Nope", cooker.backend.set_progress.call_args[0][2])
        cooker.backend.send_notif.assert_called_with(
            TEST_OBJ_TYPE, TEST_OBJ_ID)

    def test_policy_exception_cook(self):
        cooker = BaseVaultCookerMock()
        cooker.max_bundle_size = 8
        cooker.cook()

        # Potentially remove this when we have objstorage streaming
        cooker.backend.put_bundle.assert_not_called()

        cooker.backend.set_status.assert_called_with(
            TEST_OBJ_TYPE, TEST_OBJ_ID, 'failed')
        self.assertIn("exceeds", cooker.backend.set_progress.call_args[0][2])
        cooker.backend.send_notif.assert_called_with(
            TEST_OBJ_TYPE, TEST_OBJ_ID)


class TestGetTarBytes(unittest.TestCase):
    def test_tar_too_large(self):
        with tempfile.TemporaryDirectory(prefix='tmp-vault-repo-') as td:
            p = pathlib.Path(td)
            (p / 'dir1/dir2').mkdir(parents=True)
            (p / 'dir1/dir2/file').write_text('testtesttesttest')

            with self.assertRaises(BundleTooLargeError):
                    get_tar_bytes(p, size_limit=8)
