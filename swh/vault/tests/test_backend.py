# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import datetime
import psycopg2
import unittest

from unittest.mock import patch

from swh.core.tests.db_testing import DbTestFixture
from swh.model import hashutil
from swh.storage.tests.storage_testing import StorageTestFixture
from swh.vault.tests.vault_testing import VaultTestFixture


class BaseTestBackend(VaultTestFixture, StorageTestFixture, DbTestFixture):
    @contextlib.contextmanager
    def mock_cooking(self):
        with patch.object(self.vault_backend, '_send_task') as mt:
            with patch('swh.vault.backend.get_cooker') as mg:
                mcc = unittest.mock.MagicMock()
                mc = unittest.mock.MagicMock()
                mg.return_value = mcc
                mcc.return_value = mc
                mc.check_exists.return_value = True

                yield {'send_task': mt,
                       'get_cooker': mg,
                       'cooker_cls': mcc,
                       'cooker': mc}

    def assertTimestampAlmostNow(self, ts, tolerance_secs=1.0):
        now = datetime.datetime.now(datetime.timezone.utc)
        creation_delta_secs = (ts - now).total_seconds()
        self.assertLess(creation_delta_secs, tolerance_secs)


TEST_TYPE = 'revision_gitfast'
TEST_HEX_ID = '4a4b9771542143cf070386f86b4b92d42966bdbc'
TEST_OBJ_ID = hashutil.hash_to_bytes(TEST_HEX_ID)
TEST_PROGRESS = ("Mr. White, You're telling me you're cooking again?"
                 " \N{ASTONISHED FACE} ")
TEST_EMAIL = 'ouiche@example.com'


class TestBackend(BaseTestBackend, unittest.TestCase):
    def test_create_task_simple(self):
        with self.mock_cooking() as m:
            self.vault_backend.create_task(TEST_TYPE, TEST_OBJ_ID)

        m['get_cooker'].assert_called_once_with(TEST_TYPE)

        args = m['cooker_cls'].call_args[0]
        self.assertEqual(args[0], self.vault_backend.config)
        self.assertEqual(args[1], TEST_TYPE)
        self.assertEqual(args[2], TEST_OBJ_ID)

        self.assertEqual(m['cooker'].check_exists.call_count, 1)

        self.assertEqual(m['send_task'].call_count, 1)
        args = m['send_task'].call_args[0][1]
        self.assertEqual(args[0], self.vault_backend.config)
        self.assertEqual(args[1], TEST_TYPE)
        self.assertEqual(args[2], TEST_OBJ_ID)

        info = self.vault_backend.task_info(TEST_TYPE, TEST_OBJ_ID)
        self.assertEqual(info['object_id'], TEST_OBJ_ID)
        self.assertEqual(info['type'], TEST_TYPE)
        self.assertEqual(str(info['task_uuid']),
                         m['send_task'].call_args[0][0])
        self.assertEqual(info['task_status'], 'new')

        self.assertTimestampAlmostNow(info['ts_created'])

        self.assertEqual(info['ts_done'], None)
        self.assertEqual(info['progress_msg'], None)

    def test_create_fail_duplicate_task(self):
        with self.mock_cooking():
            self.vault_backend.create_task(TEST_TYPE, TEST_OBJ_ID)
            with self.assertRaises(psycopg2.IntegrityError):
                self.vault_backend.create_task(TEST_TYPE, TEST_OBJ_ID)

    def test_create_fail_nonexisting_object(self):
        with self.mock_cooking() as m:
            m['cooker'].check_exists.side_effect = ValueError('Nothing here.')
            with self.assertRaises(ValueError):
                self.vault_backend.create_task(TEST_TYPE, TEST_OBJ_ID)

    def test_create_set_progress(self):
        with self.mock_cooking():
            self.vault_backend.create_task(TEST_TYPE, TEST_OBJ_ID)

        info = self.vault_backend.task_info(TEST_TYPE, TEST_OBJ_ID)
        self.assertEqual(info['progress_msg'], None)
        self.vault_backend.set_progress(TEST_TYPE, TEST_OBJ_ID,
                                        TEST_PROGRESS)
        info = self.vault_backend.task_info(TEST_TYPE, TEST_OBJ_ID)
        self.assertEqual(info['progress_msg'], TEST_PROGRESS)

    def test_create_set_status(self):
        with self.mock_cooking():
            self.vault_backend.create_task(TEST_TYPE, TEST_OBJ_ID)

        info = self.vault_backend.task_info(TEST_TYPE, TEST_OBJ_ID)
        self.assertEqual(info['task_status'], 'new')
        self.assertEqual(info['ts_done'], None)

        self.vault_backend.set_status(TEST_TYPE, TEST_OBJ_ID, 'pending')
        info = self.vault_backend.task_info(TEST_TYPE, TEST_OBJ_ID)
        self.assertEqual(info['task_status'], 'pending')
        self.assertEqual(info['ts_done'], None)

        self.vault_backend.set_status(TEST_TYPE, TEST_OBJ_ID, 'done')
        info = self.vault_backend.task_info(TEST_TYPE, TEST_OBJ_ID)
        self.assertEqual(info['task_status'], 'done')
        self.assertTimestampAlmostNow(info['ts_done'])

    def test_create_update_access_ts(self):
        with self.mock_cooking():
            self.vault_backend.create_task(TEST_TYPE, TEST_OBJ_ID)

        info = self.vault_backend.task_info(TEST_TYPE, TEST_OBJ_ID)
        access_ts_1 = info['ts_last_access']
        self.assertTimestampAlmostNow(access_ts_1)

        self.vault_backend.update_access_ts(TEST_TYPE, TEST_OBJ_ID)
        info = self.vault_backend.task_info(TEST_TYPE, TEST_OBJ_ID)
        access_ts_2 = info['ts_last_access']
        self.assertTimestampAlmostNow(access_ts_2)

        self.vault_backend.update_access_ts(TEST_TYPE, TEST_OBJ_ID)
        info = self.vault_backend.task_info(TEST_TYPE, TEST_OBJ_ID)
        access_ts_3 = info['ts_last_access']
        self.assertTimestampAlmostNow(access_ts_3)

        self.assertLess(access_ts_1, access_ts_2)
        self.assertLess(access_ts_2, access_ts_3)

    def test_cook_request_idempotent(self):
        with self.mock_cooking():
            info1 = self.vault_backend.cook_request(TEST_TYPE, TEST_OBJ_ID)
            info2 = self.vault_backend.cook_request(TEST_TYPE, TEST_OBJ_ID)
            info3 = self.vault_backend.cook_request(TEST_TYPE, TEST_OBJ_ID)
            self.assertEqual(info1, info2)
            self.assertEqual(info1, info3)

    def test_cook_email_pending_done(self):
        with self.mock_cooking(), \
             patch.object(self.vault_backend, 'add_notif_email') as madd, \
             patch.object(self.vault_backend, 'send_notification') as msend:

            self.vault_backend.cook_request(TEST_TYPE, TEST_OBJ_ID)
            madd.assert_not_called()
            msend.assert_not_called()

            madd.reset_mock()
            msend.reset_mock()

            self.vault_backend.cook_request(TEST_TYPE, TEST_OBJ_ID, TEST_EMAIL)
            madd.assert_called_once_with(TEST_TYPE, TEST_OBJ_ID, TEST_EMAIL)
            msend.assert_not_called()

            madd.reset_mock()
            msend.reset_mock()

            self.vault_backend.set_status(TEST_TYPE, TEST_OBJ_ID, 'done')
            self.vault_backend.cook_request(TEST_TYPE, TEST_OBJ_ID, TEST_EMAIL)
            msend.assert_called_once_with(None, TEST_EMAIL,
                                          TEST_TYPE, TEST_OBJ_ID)
            madd.assert_not_called()

    def test_send_all_emails(self):
        with self.mock_cooking():
            emails = ('a@example.com',
                      'billg@example.com',
                      'test+42@example.org')
            for email in emails:
                self.vault_backend.cook_request(TEST_TYPE, TEST_OBJ_ID, email)

        self.vault_backend.set_status(TEST_TYPE, TEST_OBJ_ID, 'done')

        with patch.object(self.vault_backend, 'smtp_server') as m:
            self.vault_backend.send_all_notifications(TEST_TYPE, TEST_OBJ_ID)

            sent_emails = {k[0][0] for k in m.send_message.call_args_list}
            self.assertEqual({k['To'] for k in sent_emails}, set(emails))

            for e in sent_emails:
                self.assertIn('info@softwareheritage.org', e['From'])
                self.assertIn(TEST_TYPE, e['Subject'])
                self.assertIn(TEST_HEX_ID[:5], e['Subject'])
                self.assertIn(TEST_TYPE, str(e))
                self.assertIn('https://archive.softwareheritage.org/', str(e))
                self.assertIn(TEST_HEX_ID[:5], str(e))
                self.assertIn('--\x20\n', str(e))  # Well-formated signature!!!

            # Check that the entries have been deleted and recalling the
            # function does not re-send the e-mails
            m.reset_mock()
            self.vault_backend.send_all_notifications(TEST_TYPE, TEST_OBJ_ID)
            m.assert_not_called()
