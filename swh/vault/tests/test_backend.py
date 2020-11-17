# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import datetime
from unittest.mock import MagicMock, patch

import attr
import psycopg2
import pytest

from swh.model import hashutil
from swh.vault.exc import NotFoundExc
from swh.vault.tests.vault_testing import hash_content


@contextlib.contextmanager
def mock_cooking(vault_backend):
    with patch.object(vault_backend, "_send_task") as mt:
        mt.return_value = 42
        with patch("swh.vault.backend.get_cooker_cls") as mg:
            mcc = MagicMock()
            mc = MagicMock()
            mg.return_value = mcc
            mcc.return_value = mc
            mc.check_exists.return_value = True

            yield {
                "_send_task": mt,
                "get_cooker_cls": mg,
                "cooker_cls": mcc,
                "cooker": mc,
            }


def assertTimestampAlmostNow(ts, tolerance_secs=1.0):  # noqa
    now = datetime.datetime.now(datetime.timezone.utc)
    creation_delta_secs = (ts - now).total_seconds()
    assert creation_delta_secs < tolerance_secs


def fake_cook(backend, obj_type, result_content, sticky=False):
    content, obj_id = hash_content(result_content)
    with mock_cooking(backend):
        backend.create_task(obj_type, obj_id, sticky)
    backend.cache.add(obj_type, obj_id, b"content")
    backend.set_status(obj_type, obj_id, "done")
    return obj_id, content


def fail_cook(backend, obj_type, obj_id, failure_reason):
    with mock_cooking(backend):
        backend.create_task(obj_type, obj_id)
    backend.set_status(obj_type, obj_id, "failed")
    backend.set_progress(obj_type, obj_id, failure_reason)


TEST_TYPE = "revision_gitfast"
TEST_HEX_ID = "4a4b9771542143cf070386f86b4b92d42966bdbc"
TEST_OBJ_ID = hashutil.hash_to_bytes(TEST_HEX_ID)
TEST_PROGRESS = (
    "Mr. White, You're telling me you're cooking again?" " \N{ASTONISHED FACE} "
)
TEST_EMAIL = "ouiche@lorraine.fr"


@pytest.fixture
def swh_vault(swh_vault, sample_data):
    # make the vault's storage consistent with test data
    revision = attr.evolve(sample_data.revision, id=TEST_OBJ_ID)
    swh_vault.storage.revision_add([revision])
    return swh_vault


def test_create_task_simple(swh_vault):
    with mock_cooking(swh_vault) as m:
        swh_vault.create_task(TEST_TYPE, TEST_HEX_ID)

    m["get_cooker_cls"].assert_called_once_with(TEST_TYPE)

    args = m["cooker_cls"].call_args[0]
    assert args[0] == TEST_TYPE
    assert args[1] == TEST_HEX_ID

    assert m["cooker"].check_exists.call_count == 1
    assert m["_send_task"].call_count == 1

    args = m["_send_task"].call_args[0]
    assert args[0] == TEST_TYPE
    assert args[1] == TEST_HEX_ID

    info = swh_vault.progress(TEST_TYPE, TEST_HEX_ID)
    assert info["object_id"] == TEST_HEX_ID
    assert info["type"] == TEST_TYPE
    assert info["task_status"] == "new"
    assert info["task_id"] == 42

    assertTimestampAlmostNow(info["ts_created"])

    assert info["ts_done"] is None
    assert info["progress_msg"] is None


def test_create_fail_duplicate_task(swh_vault):
    with mock_cooking(swh_vault):
        swh_vault.create_task(TEST_TYPE, TEST_HEX_ID)
        with pytest.raises(psycopg2.IntegrityError):
            swh_vault.create_task(TEST_TYPE, TEST_HEX_ID)


def test_create_fail_nonexisting_object(swh_vault):
    with mock_cooking(swh_vault) as m:
        m["cooker"].check_exists.side_effect = ValueError("Nothing here.")
        with pytest.raises(ValueError):
            swh_vault.create_task(TEST_TYPE, TEST_HEX_ID)


def test_create_set_progress(swh_vault):
    with mock_cooking(swh_vault):
        swh_vault.create_task(TEST_TYPE, TEST_HEX_ID)

    info = swh_vault.progress(TEST_TYPE, TEST_HEX_ID)
    assert info["progress_msg"] is None
    swh_vault.set_progress(TEST_TYPE, TEST_HEX_ID, TEST_PROGRESS)
    info = swh_vault.progress(TEST_TYPE, TEST_HEX_ID)
    assert info["progress_msg"] == TEST_PROGRESS


def test_create_set_status(swh_vault):
    with mock_cooking(swh_vault):
        swh_vault.create_task(TEST_TYPE, TEST_HEX_ID)

    info = swh_vault.progress(TEST_TYPE, TEST_HEX_ID)
    assert info["task_status"] == "new"
    assert info["ts_done"] is None

    swh_vault.set_status(TEST_TYPE, TEST_HEX_ID, "pending")
    info = swh_vault.progress(TEST_TYPE, TEST_HEX_ID)
    assert info["task_status"] == "pending"
    assert info["ts_done"] is None

    swh_vault.set_status(TEST_TYPE, TEST_HEX_ID, "done")
    info = swh_vault.progress(TEST_TYPE, TEST_HEX_ID)
    assert info["task_status"] == "done"
    assertTimestampAlmostNow(info["ts_done"])


def test_create_update_access_ts(swh_vault):
    with mock_cooking(swh_vault):
        swh_vault.create_task(TEST_TYPE, TEST_OBJ_ID)

    info = swh_vault.progress(TEST_TYPE, TEST_OBJ_ID)
    access_ts_1 = info["ts_last_access"]
    assertTimestampAlmostNow(access_ts_1)

    swh_vault.update_access_ts(TEST_TYPE, TEST_OBJ_ID)
    info = swh_vault.progress(TEST_TYPE, TEST_OBJ_ID)
    access_ts_2 = info["ts_last_access"]
    assertTimestampAlmostNow(access_ts_2)

    swh_vault.update_access_ts(TEST_TYPE, TEST_OBJ_ID)
    info = swh_vault.progress(TEST_TYPE, TEST_OBJ_ID)

    access_ts_3 = info["ts_last_access"]
    assertTimestampAlmostNow(access_ts_3)

    assert access_ts_1 < access_ts_2
    assert access_ts_2 < access_ts_3


def test_cook_idempotent(swh_vault, sample_data):
    with mock_cooking(swh_vault):
        info1 = swh_vault.cook(TEST_TYPE, TEST_HEX_ID)
        info2 = swh_vault.cook(TEST_TYPE, TEST_HEX_ID)
        info3 = swh_vault.cook(TEST_TYPE, TEST_HEX_ID)
        assert info1 == info2
        assert info1 == info3


def test_cook_email_pending_done(swh_vault):
    with mock_cooking(swh_vault), patch.object(
        swh_vault, "add_notif_email"
    ) as madd, patch.object(swh_vault, "send_notification") as msend:

        swh_vault.cook(TEST_TYPE, TEST_HEX_ID)
        madd.assert_not_called()
        msend.assert_not_called()

        madd.reset_mock()
        msend.reset_mock()

        swh_vault.cook(TEST_TYPE, TEST_HEX_ID, email=TEST_EMAIL)
        madd.assert_called_once_with(TEST_TYPE, TEST_OBJ_ID, TEST_EMAIL)
        msend.assert_not_called()

        madd.reset_mock()
        msend.reset_mock()

        swh_vault.set_status(TEST_TYPE, TEST_HEX_ID, "done")
        swh_vault.cook(TEST_TYPE, TEST_HEX_ID, email=TEST_EMAIL)
        msend.assert_called_once_with(None, TEST_EMAIL, TEST_TYPE, TEST_HEX_ID, "done")
        madd.assert_not_called()


def test_send_all_emails(swh_vault):
    with mock_cooking(swh_vault):
        emails = ("a@example.com", "billg@example.com", "test+42@example.org")
        for email in emails:
            swh_vault.cook(TEST_TYPE, TEST_HEX_ID, email=email)

    swh_vault.set_status(TEST_TYPE, TEST_HEX_ID, "done")

    with patch.object(swh_vault, "smtp_server") as m:
        swh_vault.send_notif(TEST_TYPE, TEST_HEX_ID)

        sent_emails = {k[0][0] for k in m.send_message.call_args_list}
        assert {k["To"] for k in sent_emails} == set(emails)

        for e in sent_emails:
            assert "bot@softwareheritage.org" in e["From"]
            assert TEST_TYPE in e["Subject"]
            assert TEST_HEX_ID[:5] in e["Subject"]
            assert TEST_TYPE in str(e)
            assert "https://archive.softwareheritage.org/" in str(e)
            assert TEST_HEX_ID[:5] in str(e)
            assert "--\x20\n" in str(e)  # Well-formated signature!!!

        # Check that the entries have been deleted and recalling the
        # function does not re-send the e-mails
        m.reset_mock()
        swh_vault.send_notif(TEST_TYPE, TEST_HEX_ID)
        m.assert_not_called()


def test_available(swh_vault):
    assert not swh_vault.is_available(TEST_TYPE, TEST_HEX_ID)

    with mock_cooking(swh_vault):
        swh_vault.create_task(TEST_TYPE, TEST_HEX_ID)
    assert not swh_vault.is_available(TEST_TYPE, TEST_HEX_ID)

    swh_vault.cache.add(TEST_TYPE, TEST_HEX_ID, b"content")
    assert not swh_vault.is_available(TEST_TYPE, TEST_HEX_ID)

    swh_vault.set_status(TEST_TYPE, TEST_HEX_ID, "done")
    assert swh_vault.is_available(TEST_TYPE, TEST_HEX_ID)


def test_fetch(swh_vault):
    assert swh_vault.fetch(TEST_TYPE, TEST_HEX_ID, raise_notfound=False) is None

    with pytest.raises(
        NotFoundExc, match=f"{TEST_TYPE} {TEST_HEX_ID} is not available."
    ):
        swh_vault.fetch(TEST_TYPE, TEST_HEX_ID)

    obj_id, content = fake_cook(swh_vault, TEST_TYPE, b"content")

    info = swh_vault.progress(TEST_TYPE, obj_id)
    access_ts_before = info["ts_last_access"]

    assert swh_vault.fetch(TEST_TYPE, obj_id) == b"content"

    info = swh_vault.progress(TEST_TYPE, obj_id)
    access_ts_after = info["ts_last_access"]

    assertTimestampAlmostNow(access_ts_after)
    assert access_ts_before < access_ts_after


def test_cache_expire_oldest(swh_vault):
    r = range(1, 10)
    inserted = {}
    for i in r:
        sticky = i == 5
        content = b"content%s" % str(i).encode()
        obj_id, content = fake_cook(swh_vault, TEST_TYPE, content, sticky)
        inserted[i] = (obj_id, content)

    swh_vault.update_access_ts(TEST_TYPE, inserted[2][0])
    swh_vault.update_access_ts(TEST_TYPE, inserted[3][0])
    swh_vault.cache_expire_oldest(n=4)

    should_be_still_here = {2, 3, 5, 8, 9}
    for i in r:
        assert swh_vault.is_available(TEST_TYPE, inserted[i][0]) == (
            i in should_be_still_here
        )


def test_cache_expire_until(swh_vault):
    r = range(1, 10)
    inserted = {}
    for i in r:
        sticky = i == 5
        content = b"content%s" % str(i).encode()
        obj_id, content = fake_cook(swh_vault, TEST_TYPE, content, sticky)
        inserted[i] = (obj_id, content)

        if i == 7:
            cutoff_date = datetime.datetime.now()

    swh_vault.update_access_ts(TEST_TYPE, inserted[2][0])
    swh_vault.update_access_ts(TEST_TYPE, inserted[3][0])
    swh_vault.cache_expire_until(date=cutoff_date)

    should_be_still_here = {2, 3, 5, 8, 9}
    for i in r:
        assert swh_vault.is_available(TEST_TYPE, inserted[i][0]) == (
            i in should_be_still_here
        )


def test_fail_cook_simple(swh_vault):
    fail_cook(swh_vault, TEST_TYPE, TEST_HEX_ID, "error42")
    assert not swh_vault.is_available(TEST_TYPE, TEST_HEX_ID)
    info = swh_vault.progress(TEST_TYPE, TEST_HEX_ID)
    assert info["progress_msg"] == "error42"


def test_send_failure_email(swh_vault):
    with mock_cooking(swh_vault):
        swh_vault.cook(TEST_TYPE, TEST_HEX_ID, email="a@example.com")

    swh_vault.set_status(TEST_TYPE, TEST_HEX_ID, "failed")
    swh_vault.set_progress(TEST_TYPE, TEST_HEX_ID, "test error")

    with patch.object(swh_vault, "smtp_server") as m:
        swh_vault.send_notif(TEST_TYPE, TEST_HEX_ID)

        e = [k[0][0] for k in m.send_message.call_args_list][0]
        assert e["To"] == "a@example.com"

        assert "bot@softwareheritage.org" in e["From"]
        assert TEST_TYPE in e["Subject"]
        assert TEST_HEX_ID[:5] in e["Subject"]
        assert "fail" in e["Subject"]
        assert TEST_TYPE in str(e)
        assert TEST_HEX_ID[:5] in str(e)
        assert "test error" in str(e)
        assert "--\x20\n" in str(e)  # Well-formated signature


def test_retry_failed_bundle(swh_vault):
    fail_cook(swh_vault, TEST_TYPE, TEST_HEX_ID, "error42")
    info = swh_vault.progress(TEST_TYPE, TEST_HEX_ID)
    assert info["task_status"] == "failed"
    with mock_cooking(swh_vault):
        swh_vault.cook(TEST_TYPE, TEST_HEX_ID)
    info = swh_vault.progress(TEST_TYPE, TEST_HEX_ID)
    assert info["task_status"] == "new"
