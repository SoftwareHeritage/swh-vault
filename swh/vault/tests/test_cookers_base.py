# Copyright (C) 2018-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import textwrap
from unittest.mock import MagicMock

from swh.model.swhids import CoreSWHID
from swh.vault.cookers.base import BaseVaultCooker

TEST_BUNDLE_CHUNKS = [b"test content 1\n", b"test content 2\n", b"test content 3\n"]
TEST_BUNDLE_CONTENT = b"".join(TEST_BUNDLE_CHUNKS)
TEST_BUNDLE_TYPE = "test_type"
TEST_SWHID = CoreSWHID.from_string("swh:1:cnt:17a3e48bce37be5226490e750202ad3a9a1a3fe9")


class BaseVaultCookerMock(BaseVaultCooker):
    BUNDLE_TYPE = TEST_BUNDLE_TYPE

    def __init__(self):
        # we do not call super() here to bypass the building of db objects from
        # config since we do mock these db objects
        self.config = {}
        self.storage = MagicMock()
        self.backend = MagicMock()
        self.swhid = TEST_SWHID
        self.obj_id = TEST_SWHID.object_id
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
        TEST_BUNDLE_TYPE, TEST_SWHID, TEST_BUNDLE_CONTENT
    )
    cooker.backend.set_status.assert_called_with(TEST_BUNDLE_TYPE, TEST_SWHID, "done")
    cooker.backend.set_progress.assert_called_with(TEST_BUNDLE_TYPE, TEST_SWHID, None)
    cooker.backend.send_notif.assert_called_with(TEST_BUNDLE_TYPE, TEST_SWHID)


def test_code_exception_cook():
    cooker = BaseVaultCookerMock()
    cooker.prepare_bundle = MagicMock()
    cooker.prepare_bundle.side_effect = RuntimeError("Nope")
    cooker.cook()

    # Potentially remove this when we have objstorage streaming
    cooker.backend.put_bundle.assert_not_called()

    cooker.backend.set_status.assert_called_with(TEST_BUNDLE_TYPE, TEST_SWHID, "failed")
    assert cooker.backend.set_progress.call_args[0][2].startswith(
        textwrap.dedent(
            """\
            Internal Server Error. This incident will be reported.
            The full error was:

            Traceback (most recent call last):
            """
        )
    )
    cooker.backend.send_notif.assert_called_with(TEST_BUNDLE_TYPE, TEST_SWHID)


def test_policy_exception_cook():
    cooker = BaseVaultCookerMock()
    cooker.max_bundle_size = 8
    cooker.cook()

    # Potentially remove this when we have objstorage streaming
    cooker.backend.put_bundle.assert_not_called()

    cooker.backend.set_status.assert_called_with(TEST_BUNDLE_TYPE, TEST_SWHID, "failed")
    assert "exceeds" in cooker.backend.set_progress.call_args[0][2]
    cooker.backend.send_notif.assert_called_with(TEST_BUNDLE_TYPE, TEST_SWHID)
