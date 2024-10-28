# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import contextlib
import datetime
import re
import smtplib
from unittest.mock import MagicMock, patch

import attr
from backports.entry_points_selectable import entry_points as get_entry_points
import psycopg2
import pytest
import requests

from swh.core.api import APIError
from swh.core.sentry import init_sentry
from swh.model.model import Content
from swh.model.swhids import CoreSWHID
from swh.vault.exc import NotFoundExc
from swh.vault.tests.vault_testing import hash_content

SENTRY_DSN = "https://user@example.org/1234"


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


def fake_cook(backend, bundle_type, result_content, sticky=False):
    swhid = Content.from_data(result_content).swhid()
    content, obj_id = hash_content(result_content)
    with mock_cooking(backend):
        backend.create_task(bundle_type, swhid, sticky)
    backend.cache.add(bundle_type, swhid, b"content")
    backend.set_status(bundle_type, swhid, "done")
    return swhid, content


def fail_cook(backend, bundle_type, swhid, failure_reason):
    with mock_cooking(backend):
        backend.create_task(bundle_type, swhid)
    backend.set_status(bundle_type, swhid, "failed")
    backend.set_progress(bundle_type, swhid, failure_reason)


TEST_TYPE = "git_bare"
TEST_SWHID = CoreSWHID.from_string("swh:1:rev:4a4b9771542143cf070386f86b4b92d42966bdbc")
TEST_PROGRESS = (
    "Mr. White, You're telling me you're cooking again? \N{ASTONISHED FACE} "
)
TEST_EMAIL = "ouiche@lorraine.fr"


@pytest.fixture
def swh_vault(swh_vault, sample_data):
    # make the vault's storage consistent with test data
    revision = attr.evolve(sample_data.revision, id=TEST_SWHID.object_id)
    swh_vault.storage.revision_add([revision])
    return swh_vault


def test_create_task_simple(swh_vault):
    with mock_cooking(swh_vault) as m:
        swh_vault.create_task(TEST_TYPE, TEST_SWHID)

    m["get_cooker_cls"].assert_called_once_with(TEST_TYPE, TEST_SWHID.object_type)

    args = m["cooker_cls"].call_args[0]
    assert args[0] == TEST_SWHID

    assert m["cooker"].check_exists.call_count == 1
    assert m["_send_task"].call_count == 1

    args = m["_send_task"].call_args[0]
    assert args[0] == TEST_TYPE
    assert args[1] == TEST_SWHID

    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)
    assert info["swhid"] == TEST_SWHID
    assert info["type"] == TEST_TYPE
    assert info["task_status"] == "new"
    assert info["task_id"] == 42

    assertTimestampAlmostNow(info["ts_created"])

    assert info["ts_done"] is None
    assert info["progress_msg"] is None


def test_create_fail_duplicate_task(swh_vault):
    with mock_cooking(swh_vault):
        swh_vault.create_task(TEST_TYPE, TEST_SWHID)
        with pytest.raises(psycopg2.IntegrityError):
            swh_vault.create_task(TEST_TYPE, TEST_SWHID)


def test_create_fail_nonexisting_object(swh_vault):
    with mock_cooking(swh_vault) as m:
        m["cooker"].check_exists.side_effect = ValueError("Nothing here.")
        with pytest.raises(ValueError):
            swh_vault.create_task(TEST_TYPE, TEST_SWHID)


def test_create_set_progress(swh_vault):
    with mock_cooking(swh_vault):
        swh_vault.create_task(TEST_TYPE, TEST_SWHID)

    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)
    assert info["progress_msg"] is None
    swh_vault.set_progress(TEST_TYPE, TEST_SWHID, TEST_PROGRESS)
    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)
    assert info["progress_msg"] == TEST_PROGRESS


def test_create_set_status(swh_vault):
    with mock_cooking(swh_vault):
        swh_vault.create_task(TEST_TYPE, TEST_SWHID)

    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)
    assert info["task_status"] == "new"
    assert info["ts_done"] is None

    swh_vault.set_status(TEST_TYPE, TEST_SWHID, "pending")
    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)
    assert info["task_status"] == "pending"
    assert info["ts_done"] is None

    swh_vault.set_status(TEST_TYPE, TEST_SWHID, "done")
    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)
    assert info["task_status"] == "done"
    assertTimestampAlmostNow(info["ts_done"])


def test_create_update_access_ts(swh_vault):
    with mock_cooking(swh_vault):
        swh_vault.create_task(TEST_TYPE, TEST_SWHID)

    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)
    access_ts_1 = info["ts_last_access"]
    assertTimestampAlmostNow(access_ts_1)

    swh_vault.update_access_ts(TEST_TYPE, TEST_SWHID)
    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)
    access_ts_2 = info["ts_last_access"]
    assertTimestampAlmostNow(access_ts_2)

    swh_vault.update_access_ts(TEST_TYPE, TEST_SWHID)
    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)

    access_ts_3 = info["ts_last_access"]
    assertTimestampAlmostNow(access_ts_3)

    assert access_ts_1 < access_ts_2
    assert access_ts_2 < access_ts_3


def test_create_scheduler_rpc_failure(swh_vault, mocker):
    mocker.patch.object(swh_vault, "_send_task").side_effect = APIError(
        "Could not connect to scheduler RPC service"
    )
    # exception should bubble up when requesting to cook a bundle
    with pytest.raises(APIError):
        cooking_info = swh_vault.cook(TEST_TYPE, TEST_SWHID)

    # once scheduler rpc issue fixed, task should be created
    with mock_cooking(swh_vault):
        cooking_info = swh_vault.cook(TEST_TYPE, TEST_SWHID)
        assert cooking_info["task_status"] == "new"


def test_cook_idempotent(swh_vault, sample_data):
    with mock_cooking(swh_vault):
        info1 = swh_vault.cook(TEST_TYPE, TEST_SWHID)
        info2 = swh_vault.cook(TEST_TYPE, TEST_SWHID)
        info3 = swh_vault.cook(TEST_TYPE, TEST_SWHID)
        assert info1 == info2
        assert info1 == info3


def test_cook_email_pending_done(swh_vault):
    with (
        mock_cooking(swh_vault),
        patch.object(swh_vault, "add_notif_email") as madd,
        patch.object(swh_vault, "send_notification") as msend,
    ):
        swh_vault.cook(TEST_TYPE, TEST_SWHID)
        madd.assert_not_called()
        msend.assert_not_called()

        madd.reset_mock()
        msend.reset_mock()

        swh_vault.cook(TEST_TYPE, TEST_SWHID, email=TEST_EMAIL)
        madd.assert_called_once_with(TEST_TYPE, TEST_SWHID, TEST_EMAIL)
        msend.assert_not_called()

        madd.reset_mock()
        msend.reset_mock()

        swh_vault.set_status(TEST_TYPE, TEST_SWHID, "done")
        swh_vault.cache.add(TEST_TYPE, TEST_SWHID, b"content")
        swh_vault.cook(TEST_TYPE, TEST_SWHID, email=TEST_EMAIL)
        msend.assert_called_once_with(None, TEST_EMAIL, TEST_TYPE, TEST_SWHID, "done")
        madd.assert_not_called()


def test_send_all_emails(swh_vault):
    with mock_cooking(swh_vault):
        emails = ("a@example.com", "billg@example.com", "test+42@example.org")
        for email in emails:
            swh_vault.cook(TEST_TYPE, TEST_SWHID, email=email)

    swh_vault.set_status(TEST_TYPE, TEST_SWHID, "done")

    with patch.object(swh_vault, "_smtp_send") as m:
        swh_vault.send_notif(TEST_TYPE, TEST_SWHID)

        sent_emails = {k[0][0] for k in m.call_args_list}
        assert {k["To"] for k in sent_emails} == set(emails)

        download_url = (
            "https://archive.softwareheritage.org/api/1/vault/"
            f"{TEST_TYPE.replace('_', '-')}/{str(TEST_SWHID)}/raw"
        )

        for e in sent_emails:
            assert "bot@softwareheritage.org" in e["From"]
            assert TEST_TYPE in e["Subject"]
            assert TEST_SWHID.object_id.hex()[:5] in e["Subject"]
            assert TEST_TYPE in str(e)
            assert download_url in str(e)
            assert TEST_SWHID.object_id.hex()[:5] in str(e)
            assert "--\x20\n" in str(e)  # Well-formated signature!!!

        # Check that the entries have been deleted and recalling the
        # function does not re-send the e-mails
        m.reset_mock()
        swh_vault.send_notif(TEST_TYPE, TEST_SWHID)
        m.assert_not_called()


def test_send_all_emails_custom_notif(swh_vault_custom_notif):
    swh_vault = swh_vault_custom_notif
    with mock_cooking(swh_vault):
        emails = ("a@example.com", "billg@example.com", "test+42@example.org")
        for email in emails:
            swh_vault.cook(TEST_TYPE, TEST_SWHID, email=email)

    swh_vault.set_status(TEST_TYPE, TEST_SWHID, "done")

    with patch.object(swh_vault, "_smtp_send") as m:
        swh_vault.send_notif(TEST_TYPE, TEST_SWHID)

        sent_emails = {k[0][0] for k in m.call_args_list}
        assert {k["To"] for k in sent_emails} == set(emails)

        download_url = (
            "http://test.local/api/1/vault/"
            f"{TEST_TYPE.replace('_', '-')}/{str(TEST_SWHID)}/raw"
        )

        for e in sent_emails:
            assert "nobody@nowhere.local" in e["From"]
            assert TEST_TYPE in e["Subject"]
            assert TEST_SWHID.object_id.hex()[:5] in e["Subject"]
            assert TEST_TYPE in str(e)
            assert download_url in str(e)
            assert TEST_SWHID.object_id.hex()[:5] in str(e)
            assert "--\x20\n" in str(e)  # Well-formated signature!!!

        # Check that the entries have been deleted and recalling the
        # function does not re-send the e-mails
        m.reset_mock()
        swh_vault.send_notif(TEST_TYPE, TEST_SWHID)
        m.assert_not_called()


def test_send_email_error_no_smtp(swh_vault):
    reports = []
    init_sentry(SENTRY_DSN, extra_kwargs={"transport": reports.append})

    emails = ("a@example.com", "billg@example.com", "test+42@example.org")
    with mock_cooking(swh_vault):
        for email in emails:
            swh_vault.cook(TEST_TYPE, TEST_SWHID, email=email)
    swh_vault.set_status(TEST_TYPE, TEST_SWHID, "done")
    swh_vault.send_notif(TEST_TYPE, TEST_SWHID)

    assert len(reports) == 6
    for i, email in enumerate(emails):
        # first report is the logger.error
        assert reports[2 * i]["level"] == "error"
        assert reports[2 * i]["logger"] == "swh.vault.backend"
        reg = re.compile(
            f"Unable to send SMTP message 'Bundle ready: {TEST_TYPE} [0-9a-f]{{7}}' "
            f"to {email.replace('+', '[+]')}: cannot connect to server"
        )
        assert reg.match(reports[2 * i]["logentry"]["message"])
        # second is the sentry_sdk.capture_message
        assert reports[2 * i + 1]["level"] == "error"
        assert reg.match(reports[2 * i + 1]["message"])


def test_send_email_error_send_failed(swh_vault):
    reports = []
    init_sentry(SENTRY_DSN, extra_kwargs={"transport": reports.append})

    emails = ("a@example.com", "billg@example.com", "test+42@example.org")
    with mock_cooking(swh_vault):
        for email in emails:
            swh_vault.cook(TEST_TYPE, TEST_SWHID, email=email)
    swh_vault.set_status(TEST_TYPE, TEST_SWHID, "done")

    with patch("smtplib.SMTP") as MockSMTP:
        smtp = MockSMTP.return_value
        smtp.noop.return_value = [250]
        smtp.send_message.side_effect = smtplib.SMTPHeloError(404, "HELO Failed")

        swh_vault.send_notif(TEST_TYPE, TEST_SWHID)

    assert len(reports) == 4
    # first one is the captured exception
    assert reports[0]["level"] == "error"
    assert reports[0]["exception"]["values"][0]["type"] == "SMTPHeloError"

    # the following 3 ones are the sentry_sdk.capture_message() calls
    for i, email in enumerate(emails, start=1):
        assert reports[i]["level"] == "error"
        reg = re.compile(
            f"Unable to send SMTP message 'Bundle ready: {TEST_TYPE} [0-9a-f]{{7}}' "
            f"to {email.replace('+', '[+]')}: [(]404, 'HELO Failed'[)]"
        )
        assert reg.match(reports[i]["message"])


def test_available(swh_vault):
    assert not swh_vault.is_available(TEST_TYPE, TEST_SWHID)

    with mock_cooking(swh_vault):
        swh_vault.create_task(TEST_TYPE, TEST_SWHID)
    assert not swh_vault.is_available(TEST_TYPE, TEST_SWHID)

    swh_vault.cache.add(TEST_TYPE, TEST_SWHID, b"content")
    assert not swh_vault.is_available(TEST_TYPE, TEST_SWHID)

    swh_vault.set_status(TEST_TYPE, TEST_SWHID, "done")
    assert swh_vault.is_available(TEST_TYPE, TEST_SWHID)


def test_fetch(swh_vault):
    assert swh_vault.fetch(TEST_TYPE, TEST_SWHID, raise_notfound=False) is None

    with pytest.raises(
        NotFoundExc, match=f"{TEST_TYPE} {TEST_SWHID} is not available."
    ):
        swh_vault.fetch(TEST_TYPE, TEST_SWHID)

    swhid, content = fake_cook(swh_vault, TEST_TYPE, b"content")

    info = swh_vault.progress(TEST_TYPE, swhid)
    access_ts_before = info["ts_last_access"]

    assert swh_vault.fetch(TEST_TYPE, swhid) == b"content"

    info = swh_vault.progress(TEST_TYPE, swhid)
    access_ts_after = info["ts_last_access"]

    assertTimestampAlmostNow(access_ts_after)
    assert access_ts_before < access_ts_after


def test_cache_expire_oldest(swh_vault):
    r = range(1, 10)
    inserted = {}
    for i in r:
        sticky = i == 5
        content = b"content%s" % str(i).encode()
        swhid, content = fake_cook(swh_vault, TEST_TYPE, content, sticky)
        inserted[i] = (swhid, content)

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
        swhid, content = fake_cook(swh_vault, TEST_TYPE, content, sticky)
        inserted[i] = (swhid, content)

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
    fail_cook(swh_vault, TEST_TYPE, TEST_SWHID, "error42")
    assert not swh_vault.is_available(TEST_TYPE, TEST_SWHID)
    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)
    assert info["progress_msg"] == "error42"


def test_send_failure_email(swh_vault):
    with mock_cooking(swh_vault):
        swh_vault.cook(TEST_TYPE, TEST_SWHID, email="a@example.com")

    swh_vault.set_status(TEST_TYPE, TEST_SWHID, "failed")
    swh_vault.set_progress(TEST_TYPE, TEST_SWHID, "test error")

    with patch.object(swh_vault, "_smtp_send") as m:
        swh_vault.send_notif(TEST_TYPE, TEST_SWHID)

        e = [k[0][0] for k in m.call_args_list][0]
        assert e["To"] == "a@example.com"

        assert "bot@softwareheritage.org" in e["From"]
        assert TEST_TYPE in e["Subject"]
        assert TEST_SWHID.object_id.hex()[:5] in e["Subject"]
        assert "fail" in e["Subject"]
        assert TEST_TYPE in str(e)
        assert TEST_SWHID.object_id.hex()[:5] in str(e)
        assert "test error" in str(e)
        assert "--\x20\n" in str(e)  # Well-formated signature


def test_retry_failed_bundle(swh_vault):
    fail_cook(swh_vault, TEST_TYPE, TEST_SWHID, "error42")
    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)
    assert info["task_status"] == "failed"
    with mock_cooking(swh_vault):
        swh_vault.cook(TEST_TYPE, TEST_SWHID)
    info = swh_vault.progress(TEST_TYPE, TEST_SWHID)
    assert info["task_status"] == "new"


def test_download_url_cache_pathslicing_backend(swh_vault):
    swhid, content = fake_cook(swh_vault, TEST_TYPE, b"content")
    # download URL feature is not available with pathslicing backend for vault cache
    assert swh_vault.download_url(TEST_TYPE, swhid) is None


@pytest.fixture
def swh_vault_config_http_cache(swh_vault_config, httpserver):
    swh_vault_config["cache"] = {
        "cls": "http",
        "url": httpserver.url_for("/"),
        "compression": "none",
    }
    return swh_vault_config


@pytest.fixture
def swh_vault_http_cache(swh_vault_config_http_cache):
    from swh.vault import get_vault

    return get_vault("postgresql", **swh_vault_config_http_cache)


def test_download_url_cache_http_backend(swh_vault_http_cache, mocker, httpserver):
    unknown_swhid = Content.from_data(b"foo").swhid()
    with pytest.raises(
        NotFoundExc, match=f"{TEST_TYPE} {unknown_swhid} is not available."
    ):
        swh_vault_http_cache.download_url(TEST_TYPE, unknown_swhid)

    mocker.patch.object(
        swh_vault_http_cache, "progress", return_value={"task_status": "done"}
    )
    content = b"content"
    swhid = Content.from_data(content).swhid()
    objid = swh_vault_http_cache.cache._get_internal_id(TEST_TYPE, swhid)["sha1"]
    httpserver.expect_request(f"/{objid.hex()}").respond_with_data(content)
    download_url = swh_vault_http_cache.download_url(TEST_TYPE, swhid)
    assert download_url is not None
    assert requests.get(download_url).content == content


def test_cook_if_status_done_but_bundle_not_in_cache(swh_vault, mocker):
    with mock_cooking(swh_vault):
        swh_vault.cook(TEST_TYPE, TEST_SWHID, email="a@example.com")
    swh_vault.cache.add(TEST_TYPE, TEST_SWHID, b"content")
    swh_vault.set_status(TEST_TYPE, TEST_SWHID, "done")

    create_task = mocker.spy(swh_vault, "create_task")

    with mock_cooking(swh_vault):
        swh_vault.cook(TEST_TYPE, TEST_SWHID, email="a@example.com")

    create_task.assert_not_called()

    swh_vault.cache.delete(TEST_TYPE, TEST_SWHID)

    with mock_cooking(swh_vault):
        swh_vault.cook(TEST_TYPE, TEST_SWHID, email="a@example.com")

    create_task.assert_called_once_with(TEST_TYPE, TEST_SWHID, False)


def test_registered_backends():

    entry_points = get_entry_points(group="swh.vault.classes")
    assert {ep.name for ep in entry_points} == {"remote", "postgresql", "memory"}
    for ep in entry_points:
        assert ep.load()
