# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
from datetime import timedelta
from email.mime.text import MIMEText
import logging
import smtplib
from typing import Any, Dict, List, Optional, Tuple

import psycopg2.extras
import psycopg2.pool
import sentry_sdk

from swh.core.db import BaseDb
from swh.core.db.common import db_transaction
from swh.model.swhids import CoreSWHID
from swh.scheduler import get_scheduler
from swh.scheduler.utils import create_oneshot_task
from swh.storage import get_storage
from swh.vault.cache import VaultCache
from swh.vault.cookers import COOKER_TYPES, get_cooker_cls
from swh.vault.exc import NotFoundExc

logger = logging.getLogger(__name__)
cooking_task_name = "swh.vault.cooking_tasks.SWHCookingTask"

NOTIF_EMAIL_FROM = '"Software Heritage Vault" ' "<bot@softwareheritage.org>"
NOTIF_EMAIL_SUBJECT_SUCCESS = "Bundle ready: {bundle_type} {short_id}"
NOTIF_EMAIL_SUBJECT_FAILURE = "Bundle failed: {bundle_type} {short_id}"

NOTIF_EMAIL_BODY_SUCCESS = """
You have requested the following bundle from the Software Heritage
Vault:

Bundle Type: {bundle_type}
Object SWHID: {swhid}

This bundle is now available for download at the following address:

{url}

Please keep in mind that this link might expire at some point, in which
case you will need to request the bundle again.

--\x20
The Software Heritage Developers
"""

NOTIF_EMAIL_BODY_FAILURE = """
You have requested the following bundle from the Software Heritage
Vault:

Bundle Type: {bundle_type}
Object SWHID: {swhid}

This bundle could not be cooked for the following reason:

{progress_msg}

We apologize for the inconvenience.

--\x20
The Software Heritage Developers
"""


class VaultDB:
    """
    PostgreSQL backend for the Software Heritage Vault.
    """

    current_version = 4

    def __init__(self, **config):
        self.config = config

        if "db" not in self.config:
            raise ValueError(
                "The 'db' configuration entry is missing "
                "in the vault configuration file"
            )
        db_conn = config["db"]
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            config.get("min_pool_conns", 1),
            config.get("max_pool_conns", 10),
            db_conn,
            cursor_factory=psycopg2.extras.RealDictCursor,
        )
        self._db = None

    def get_db(self):
        if self._db:
            return self._db
        return BaseDb.from_pool(self._pool)

    def put_db(self, db):
        if db is not self._db:
            db.put_conn()


class VaultBackend(VaultDB):
    """
    Backend for the Software Heritage Vault.
    """

    def __init__(self, **config):
        super().__init__(**config)
        self.cache = VaultCache(**config["cache"])
        self.scheduler = get_scheduler(**config["scheduler"])
        self.storage = get_storage(**config["storage"])

    @db_transaction()
    def progress(
        self,
        bundle_type: str,
        swhid: CoreSWHID,
        raise_notfound: bool = True,
        db=None,
        cur=None,
    ) -> Optional[Dict[str, Any]]:
        cur.execute(
            """
            SELECT id, type, swhid, task_id, task_status, sticky,
                   ts_created, ts_done, ts_last_access, progress_msg
            FROM vault_bundle
            WHERE type = %s AND swhid = %s""",
            (bundle_type, str(swhid)),
        )
        res = cur.fetchone()
        if not res:
            if raise_notfound:
                raise NotFoundExc(f"{bundle_type} {swhid} was not found.")
            return None
        res["swhid"] = CoreSWHID.from_string(res["swhid"])
        return res

    def _send_task(self, bundle_type: str, swhid: CoreSWHID):
        """Send a cooking task to the celery scheduler"""
        task = create_oneshot_task("cook-vault-bundle", bundle_type, str(swhid))
        added_tasks = self.scheduler.create_tasks([task])
        return added_tasks[0].id

    @db_transaction()
    def create_task(
        self,
        bundle_type: str,
        swhid: CoreSWHID,
        sticky: bool = False,
        db=None,
        cur=None,
    ):
        """Create and send a cooking task"""
        cooker_class = get_cooker_cls(bundle_type, swhid.object_type)
        cooker = cooker_class(swhid, backend=self, storage=self.storage)

        if not cooker.check_exists():
            raise NotFoundExc(f"{bundle_type} {swhid} was not found.")

        task_id = self._send_task(bundle_type, swhid)

        cur.execute(
            """
            INSERT INTO vault_bundle (type, swhid, sticky, task_id)
            VALUES (%s, %s, %s, %s)""",
            (bundle_type, str(swhid), sticky, task_id),
        )

    @db_transaction()
    def add_notif_email(
        self, bundle_type: str, swhid: CoreSWHID, email: str, db=None, cur=None
    ):
        """Add an e-mail address to notify when a given bundle is ready"""
        cur.execute(
            """
            INSERT INTO vault_notif_email (email, bundle_id)
            VALUES (%s, (SELECT id FROM vault_bundle
                         WHERE type = %s AND swhid = %s))""",
            (email, bundle_type, str(swhid)),
        )

    def put_bundle(self, bundle_type: str, swhid: CoreSWHID, bundle) -> bool:
        self.cache.add(bundle_type, swhid, bundle)
        return True

    @db_transaction()
    def cook(
        self,
        bundle_type: str,
        swhid: CoreSWHID,
        *,
        sticky: bool = False,
        email: Optional[str] = None,
        db=None,
        cur=None,
    ) -> Dict[str, Any]:
        info = self.progress(bundle_type, swhid, raise_notfound=False)

        if bundle_type not in COOKER_TYPES:
            raise NotFoundExc(f"{bundle_type} is an unknown type.")

        if info is not None and (
            info["task_status"] == "failed"
            or (
                info["task_status"] == "done"
                and not self.cache.is_cached(bundle_type, swhid)
            )
        ):
            # If there's a failed bundle entry or bundle no longer in cache, delete it first.
            cur.execute(
                "DELETE FROM vault_bundle WHERE type = %s AND swhid = %s",
                (bundle_type, str(swhid)),
            )
            db.conn.commit()
            info = None

        # If there's no bundle entry, create the task.
        if info is None:
            self.create_task(bundle_type, swhid, sticky)

        if email is not None:
            # If the task is already done, send the email directly
            if info is not None and info["task_status"] == "done":
                self.send_notification(
                    None, email, bundle_type, swhid, info["task_status"]
                )
            # Else, add it to the notification queue
            else:
                self.add_notif_email(bundle_type, swhid, email)

        return self.progress(bundle_type, swhid)

    @db_transaction()
    def batch_cook(
        self, batch: List[Tuple[str, str]], db=None, cur=None
    ) -> Dict[str, int]:
        # Import execute_values at runtime only, because it requires
        # psycopg2 >= 2.7 (only available on postgresql servers)
        from psycopg2.extras import execute_values

        for bundle_type, _ in batch:
            if bundle_type not in COOKER_TYPES:
                raise NotFoundExc(f"{bundle_type} is an unknown type.")

        cur.execute(
            """
            INSERT INTO vault_batch (id)
            VALUES (DEFAULT)
            RETURNING id"""
        )
        batch_id = cur.fetchone()["id"]

        # Delete all failed bundles from the batch
        cur.execute(
            """
            DELETE FROM vault_bundle
            WHERE task_status = 'failed'
              AND (type, swhid) IN %s""",
            (tuple(batch),),
        )

        # Insert all the bundles, return the new ones
        execute_values(
            cur,
            """
            INSERT INTO vault_bundle (type, swhid)
            VALUES %s ON CONFLICT DO NOTHING""",
            batch,
        )

        # Get the bundle ids and task status
        cur.execute(
            """
            SELECT id, type, swhid, task_id FROM vault_bundle
            WHERE (type, swhid) IN %s""",
            (tuple(batch),),
        )
        bundles = cur.fetchall()

        # Insert the batch-bundle entries
        batch_id_bundle_ids = [(batch_id, row["id"]) for row in bundles]
        execute_values(
            cur,
            """
            INSERT INTO vault_batch_bundle (batch_id, bundle_id)
            VALUES %s ON CONFLICT DO NOTHING""",
            batch_id_bundle_ids,
        )
        db.conn.commit()

        # Get the tasks to fetch
        batch_new = [
            (row["type"], CoreSWHID.from_string(row["swhid"]))
            for row in bundles
            if row["task_id"] is None
        ]

        # Send the tasks
        args_batch = [(bundle_type, swhid) for bundle_type, swhid in batch_new]
        # TODO: change once the scheduler handles priority tasks
        tasks = [
            create_oneshot_task("swh-vault-batch-cooking", *args) for args in args_batch
        ]

        added_tasks = self.scheduler.create_tasks(tasks)
        tasks_ids_bundle_ids = [
            (task_id, bundle_type, swhid)
            for task_id, (bundle_type, swhid) in zip(
                [task.id for task in added_tasks], batch_new
            )
        ]

        # Update the task ids
        execute_values(
            cur,
            """
            UPDATE vault_bundle
            SET task_id = s_task_id
            FROM (VALUES %s) AS sub (s_task_id, s_type, s_swhid)
            WHERE type = s_type::cook_type AND swhid = s_swhid """,
            tasks_ids_bundle_ids,
        )
        return {"id": batch_id}

    @db_transaction()
    def batch_progress(self, batch_id: int, db=None, cur=None) -> Dict[str, Any]:
        cur.execute(
            """
            SELECT vault_bundle.id as id,
                   type, swhid, task_id, task_status, sticky,
                   ts_created, ts_done, ts_last_access, progress_msg
            FROM vault_batch_bundle
            LEFT JOIN vault_bundle ON vault_bundle.id = bundle_id
            WHERE batch_id = %s""",
            (batch_id,),
        )
        bundles = cur.fetchall()
        if not bundles:
            raise NotFoundExc(f"Batch {batch_id} does not exist.")

        for bundle in bundles:
            bundle["swhid"] = CoreSWHID.from_string(bundle["swhid"])

        counter = collections.Counter(b["status"] for b in bundles)
        res = {
            "bundles": bundles,
            "total": len(bundles),
            **{k: 0 for k in ("new", "pending", "done", "failed")},
            **dict(counter),
        }

        return res

    @db_transaction()
    def is_available(self, bundle_type: str, swhid: CoreSWHID, db=None, cur=None):
        """Check whether a bundle is available for retrieval"""
        info = self.progress(bundle_type, swhid, raise_notfound=False, cur=cur)
        return (
            info is not None
            and info["task_status"] == "done"
            and self.cache.is_cached(bundle_type, swhid)
        )

    @db_transaction()
    def fetch(
        self, bundle_type: str, swhid: CoreSWHID, raise_notfound=True, db=None, cur=None
    ) -> Optional[bytes]:
        """Retrieve a bundle from the cache"""
        available = self.is_available(bundle_type, swhid, cur=cur)
        if not available:
            if raise_notfound:
                raise NotFoundExc(f"{bundle_type} {swhid} is not available.")
            return None
        self.update_access_ts(bundle_type, swhid, cur=cur)
        return self.cache.get(bundle_type, swhid)

    @db_transaction()
    def download_url(
        self,
        bundle_type: str,
        swhid: CoreSWHID,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
        raise_notfound=True,
        db=None,
        cur=None,
    ) -> Optional[str]:
        """Obtain a bundle direct download link from the cache if supported"""
        available = self.is_available(bundle_type, swhid, cur=cur)
        if not available:
            if raise_notfound:
                raise NotFoundExc(f"{bundle_type} {swhid} is not available.")
            return None
        return self.cache.download_url(bundle_type, swhid, content_disposition, expiry)

    @db_transaction()
    def update_access_ts(self, bundle_type: str, swhid: CoreSWHID, db=None, cur=None):
        """Update the last access timestamp of a bundle"""
        cur.execute(
            """
            UPDATE vault_bundle
            SET ts_last_access = NOW()
            WHERE type = %s AND swhid = %s""",
            (bundle_type, str(swhid)),
        )

    @db_transaction()
    def set_status(
        self, bundle_type: str, swhid: CoreSWHID, status: str, db=None, cur=None
    ) -> bool:
        req = (
            """
               UPDATE vault_bundle
               SET task_status = %s """
            + (""", ts_done = NOW() """ if status == "done" else "")
            + """WHERE type = %s AND swhid = %s"""
        )
        cur.execute(req, (status, bundle_type, str(swhid)))
        return True

    @db_transaction()
    def set_progress(
        self, bundle_type: str, swhid: CoreSWHID, progress: str, db=None, cur=None
    ) -> bool:
        cur.execute(
            """
            UPDATE vault_bundle
            SET progress_msg = %s
            WHERE type = %s AND swhid = %s""",
            (progress, bundle_type, str(swhid)),
        )
        return True

    @db_transaction()
    def send_notif(self, bundle_type: str, swhid: CoreSWHID, db=None, cur=None) -> bool:
        cur.execute(
            """
            SELECT vault_notif_email.id AS id, email, task_status, progress_msg
            FROM vault_notif_email
            INNER JOIN vault_bundle ON bundle_id = vault_bundle.id
            WHERE vault_bundle.type = %s AND vault_bundle.swhid = %s""",
            (bundle_type, str(swhid)),
        )
        for d in cur:
            self.send_notification(
                d["id"],
                d["email"],
                bundle_type,
                swhid,
                status=d["task_status"],
                progress_msg=d["progress_msg"],
            )
        return True

    @db_transaction()
    def send_notification(
        self,
        n_id: Optional[int],
        email: str,
        bundle_type: str,
        swhid: CoreSWHID,
        status: str,
        progress_msg: Optional[str] = None,
        db=None,
        cur=None,
    ) -> None:
        """Send the notification of a bundle to a specific e-mail"""
        short_id = swhid.object_id.hex()[:7]

        # TODO: instead of hardcoding this, we should probably:
        # * add a "fetch_url" field in the vault_notif_email table
        # * generate the url with flask.url_for() on the web-ui side
        # * send this url as part of the cook request and store it in
        #   the table
        # * use this url for the notification e-mail
        # UPDATE: for now, let's just retrieve the URL from a config entry, if
        # any, so we can use it on mirror instances
        base_url = self.config.get("notification", {}).get(
            "api_url", "https://archive.softwareheritage.org/api/1"
        )
        url = f"{base_url}/vault/{bundle_type.replace('_', '-')}/{swhid}/raw"

        if status == "done":
            text = NOTIF_EMAIL_BODY_SUCCESS.strip()
            text = text.format(bundle_type=bundle_type, swhid=swhid, url=url)
            msg = MIMEText(text)
            msg["Subject"] = NOTIF_EMAIL_SUBJECT_SUCCESS.format(
                bundle_type=bundle_type, short_id=short_id
            )
        elif status == "failed":
            text = NOTIF_EMAIL_BODY_FAILURE.strip()
            text = text.format(
                bundle_type=bundle_type, swhid=swhid, progress_msg=progress_msg
            )
            msg = MIMEText(text)
            msg["Subject"] = NOTIF_EMAIL_SUBJECT_FAILURE.format(
                bundle_type=bundle_type, short_id=short_id
            )
        else:
            raise RuntimeError(
                "send_notification called on a '{}' bundle".format(status)
            )

        msg["From"] = self.config.get("notification", {}).get("from", NOTIF_EMAIL_FROM)
        msg["To"] = email

        self._smtp_send(msg)

        if n_id is not None:
            cur.execute(
                """
                DELETE FROM vault_notif_email
                WHERE id = %s""",
                (n_id,),
            )

    def _smtp_send(self, msg: MIMEText):
        smtp_server = smtplib.SMTP(**self.config.get("smtp", {}))
        try:
            status = smtp_server.noop()[0]
        except smtplib.SMTPException as e:
            error_message = (
                f"Unable to send SMTP message '{msg['Subject']}' to "
                f"{msg['To']}: cannot connect to server ({e})"
            )
            logger.error(error_message)
            sentry_sdk.capture_message(error_message, "error")
        else:
            if status != 250:
                error_message = (
                    f"Unable to send SMTP message '{msg['Subject']}' to "
                    f"{msg['To']}: server returned status {status}"
                )
                logger.error(error_message)
                sentry_sdk.capture_message(error_message, "error")
            else:
                try:
                    # Send the message
                    smtp_server.send_message(msg)
                except smtplib.SMTPException as exc:
                    logger.exception(exc)
                    error_message = (
                        f"Unable to send SMTP message '{msg['Subject']}' to "
                        f"{msg['To']}: {exc}"
                    )
                    sentry_sdk.capture_message(error_message, "error")

    @db_transaction()
    def _cache_expire(self, cond, *args, db=None, cur=None) -> None:
        """Low-level expiration method, used by cache_expire_* methods"""
        # Embedded SELECT query to be able to use ORDER BY and LIMIT
        cur.execute(
            """
            DELETE FROM vault_bundle
            WHERE ctid IN (
                SELECT ctid
                FROM vault_bundle
                WHERE sticky = false
                {}
            )
            RETURNING type, swhid
            """.format(
                cond
            ),
            args,
        )

        for d in cur:
            self.cache.delete(d["type"], CoreSWHID.from_string(d["swhid"]))

    @db_transaction()
    def cache_expire_oldest(self, n=1, by="last_access", db=None, cur=None) -> None:
        """Expire the `n` oldest bundles"""
        assert by in ("created", "done", "last_access")
        filter = """ORDER BY ts_{} LIMIT {}""".format(by, n)
        return self._cache_expire(filter)

    @db_transaction()
    def cache_expire_until(self, date, by="last_access", db=None, cur=None) -> None:
        """Expire all the bundles until a certain date"""
        assert by in ("created", "done", "last_access")
        filter = """AND ts_{} <= %s""".format(by)
        return self._cache_expire(filter, date)
