# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from unittest.mock import MagicMock

from swh.model import hashutil
from swh.vault.cookers.base import BaseVaultCooker


TEST_BUNDLE_CHUNKS = [b"test content 1\n",
                      b"test content 2\n",
                      b"test content 3\n"]
TEST_BUNDLE_CONTENT = b''.join(TEST_BUNDLE_CHUNKS)
TEST_OBJ_TYPE = 'test_type'
TEST_HEX_ID = '17a3e48bce37be5226490e750202ad3a9a1a3fe9'
TEST_OBJ_ID = hashutil.hash_to_bytes(TEST_HEX_ID)


class BaseVaultCookerMock(BaseVaultCooker):
    CACHE_TYPE_KEY = TEST_OBJ_TYPE

    def __init__(self):
        # we do not call super() here to bypass the building of db objects from
        # config since we do mock these db objects
        self.config = {}
        self.storage = MagicMock()
        self.backend = MagicMock()
        self.obj_type = self.CACHE_TYPE_KEY
        self.obj_id = hashutil.hash_to_bytes(TEST_OBJ_ID)
        self.max_bundle_size = 1024

    def check_exists(self):
        return True

    def prepare_bundle(self):
        for chunk in TEST_BUNDLE_CHUNKS:
            self.write(chunk)


def test_simple_cook():
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


def test_code_exception_cook():
    cooker = BaseVaultCookerMock()
    cooker.prepare_bundle = MagicMock()
    cooker.prepare_bundle.side_effect = RuntimeError("Nope")
    cooker.cook()

    # Potentially remove this when we have objstorage streaming
    cooker.backend.put_bundle.assert_not_called()

    cooker.backend.set_status.assert_called_with(
        TEST_OBJ_TYPE, TEST_OBJ_ID, 'failed')
    assert "Nope" not in cooker.backend.set_progress.call_args[0][2]
    cooker.backend.send_notif.assert_called_with(TEST_OBJ_TYPE, TEST_OBJ_ID)


def test_policy_exception_cook():
    cooker = BaseVaultCookerMock()
    cooker.max_bundle_size = 8
    cooker.cook()

    # Potentially remove this when we have objstorage streaming
    cooker.backend.put_bundle.assert_not_called()

    cooker.backend.set_status.assert_called_with(
        TEST_OBJ_TYPE, TEST_OBJ_ID, 'failed')
    assert "exceeds" in cooker.backend.set_progress.call_args[0][2]
    cooker.backend.send_notif.assert_called_with(TEST_OBJ_TYPE, TEST_OBJ_ID)
