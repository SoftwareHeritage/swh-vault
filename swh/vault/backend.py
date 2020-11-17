# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
from email.mime.text import MIMEText
import smtplib
from typing import Any, Dict, List, Optional, Tuple

import psycopg2.extras
import psycopg2.pool

from swh.core.db import BaseDb
from swh.core.db.common import db_transaction
from swh.model import hashutil
from swh.scheduler import get_scheduler
from swh.scheduler.utils import create_oneshot_task_dict
from swh.storage import get_storage
from swh.vault.cache import VaultCache
from swh.vault.cookers import COOKER_TYPES, get_cooker_cls
from swh.vault.exc import NotFoundExc
from swh.vault.interface import ObjectId

cooking_task_name = "swh.vault.cooking_tasks.SWHCookingTask"

NOTIF_EMAIL_FROM = '"Software Heritage Vault" ' "<bot@softwareheritage.org>"
NOTIF_EMAIL_SUBJECT_SUCCESS = "Bundle ready: {obj_type} {short_id}"
NOTIF_EMAIL_SUBJECT_FAILURE = "Bundle failed: {obj_type} {short_id}"

NOTIF_EMAIL_BODY_SUCCESS = """
You have requested the following bundle from the Software Heritage
Vault:

Object Type: {obj_type}
Object ID: {hex_id}

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

Object Type: {obj_type}
Object ID: {hex_id}

This bundle could not be cooked for the following reason:

{progress_msg}

We apologize for the inconvenience.

--\x20
The Software Heritage Developers
"""


def batch_to_bytes(batch: List[Tuple[str, str]]) -> List[Tuple[str, bytes]]:
    return [(obj_type, hashutil.hash_to_bytes(hex_id)) for obj_type, hex_id in batch]


class VaultBackend:
    """
    Backend for the Software Heritage vault.
    """

    def __init__(self, db, **config):
        self.config = config
        self.cache = VaultCache(**config["cache"])
        self.scheduler = get_scheduler(**config["scheduler"])
        self.storage = get_storage(**config["storage"])
        self.smtp_server = smtplib.SMTP()

        self._pool = psycopg2.pool.ThreadedConnectionPool(
            config.get("min_pool_conns", 1),
            config.get("max_pool_conns", 10),
            db,
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

    def _compute_ids(self, obj_id: ObjectId) -> Tuple[str, bytes]:
        """Internal method to reconcile multiple possible inputs

        """
        if isinstance(obj_id, str):
            return obj_id, hashutil.hash_to_bytes(obj_id)
        return hashutil.hash_to_hex(obj_id), obj_id

    @db_transaction()
    def progress(
        self,
        obj_type: str,
        obj_id: ObjectId,
        raise_notfound: bool = True,
        db=None,
        cur=None,
    ) -> Optional[Dict[str, Any]]:
        hex_id, obj_id = self._compute_ids(obj_id)
        cur.execute(
            """
            SELECT id, type, object_id, task_id, task_status, sticky,
                   ts_created, ts_done, ts_last_access, progress_msg
            FROM vault_bundle
            WHERE type = %s AND object_id = %s""",
            (obj_type, obj_id),
        )
        res = cur.fetchone()
        if not res:
            if raise_notfound:
                raise NotFoundExc(f"{obj_type} {hex_id} was not found.")
            return None

        res["object_id"] = hashutil.hash_to_hex(res["object_id"])
        return res

    def _send_task(self, obj_type: str, hex_id: ObjectId):
        """Send a cooking task to the celery scheduler"""
        task = create_oneshot_task_dict("cook-vault-bundle", obj_type, hex_id)
        added_tasks = self.scheduler.create_tasks([task])
        return added_tasks[0]["id"]

    @db_transaction()
    def create_task(
        self, obj_type: str, obj_id: bytes, sticky: bool = False, db=None, cur=None
    ):
        """Create and send a cooking task"""
        hex_id, obj_id = self._compute_ids(obj_id)

        cooker_class = get_cooker_cls(obj_type)
        cooker = cooker_class(obj_type, hex_id, backend=self, storage=self.storage)

        if not cooker.check_exists():
            raise NotFoundExc(f"{obj_type} {hex_id} was not found.")

        cur.execute(
            """
            INSERT INTO vault_bundle (type, object_id, sticky)
            VALUES (%s, %s, %s)""",
            (obj_type, obj_id, sticky),
        )
        db.conn.commit()

        task_id = self._send_task(obj_type, hex_id)

        cur.execute(
            """
            UPDATE vault_bundle
            SET task_id = %s
            WHERE type = %s AND object_id = %s""",
            (task_id, obj_type, obj_id),
        )

    @db_transaction()
    def add_notif_email(
        self, obj_type: str, obj_id: bytes, email: str, db=None, cur=None
    ):
        """Add an e-mail address to notify when a given bundle is ready"""
        cur.execute(
            """
            INSERT INTO vault_notif_email (email, bundle_id)
            VALUES (%s, (SELECT id FROM vault_bundle
                         WHERE type = %s AND object_id = %s))""",
            (email, obj_type, obj_id),
        )

    def put_bundle(self, obj_type: str, obj_id: ObjectId, bundle) -> bool:
        _, obj_id = self._compute_ids(obj_id)
        self.cache.add(obj_type, obj_id, bundle)
        return True

    @db_transaction()
    def cook(
        self,
        obj_type: str,
        obj_id: ObjectId,
        *,
        sticky: bool = False,
        email: Optional[str] = None,
        db=None,
        cur=None,
    ) -> Dict[str, Any]:
        hex_id, obj_id = self._compute_ids(obj_id)
        info = self.progress(obj_type, obj_id, raise_notfound=False)

        if obj_type not in COOKER_TYPES:
            raise NotFoundExc(f"{obj_type} is an unknown type.")

        # If there's a failed bundle entry, delete it first.
        if info is not None and info["task_status"] == "failed":
            obj_id = hashutil.hash_to_bytes(obj_id)
            cur.execute(
                "DELETE FROM vault_bundle WHERE type = %s AND object_id = %s",
                (obj_type, obj_id),
            )
            db.conn.commit()
            info = None

        # If there's no bundle entry, create the task.
        if info is None:
            self.create_task(obj_type, obj_id, sticky)

        if email is not None:
            # If the task is already done, send the email directly
            if info is not None and info["task_status"] == "done":
                self.send_notification(
                    None, email, obj_type, hex_id, info["task_status"]
                )
            # Else, add it to the notification queue
            else:
                self.add_notif_email(obj_type, obj_id, email)

        return self.progress(obj_type, obj_id)

    @db_transaction()
    def batch_cook(
        self, batch: List[Tuple[str, str]], db=None, cur=None
    ) -> Dict[str, int]:
        # Import execute_values at runtime only, because it requires
        # psycopg2 >= 2.7 (only available on postgresql servers)
        from psycopg2.extras import execute_values

        for obj_type, _ in batch:
            if obj_type not in COOKER_TYPES:
                raise NotFoundExc(f"{obj_type} is an unknown type.")

        cur.execute(
            """
            INSERT INTO vault_batch (id)
            VALUES (DEFAULT)
            RETURNING id"""
        )
        batch_id = cur.fetchone()["id"]
        batch_bytes = batch_to_bytes(batch)

        # Delete all failed bundles from the batch
        cur.execute(
            """
            DELETE FROM vault_bundle
            WHERE task_status = 'failed'
              AND (type, object_id) IN %s""",
            (tuple(batch_bytes),),
        )

        # Insert all the bundles, return the new ones
        execute_values(
            cur,
            """
            INSERT INTO vault_bundle (type, object_id)
            VALUES %s ON CONFLICT DO NOTHING""",
            batch_bytes,
        )

        # Get the bundle ids and task status
        cur.execute(
            """
            SELECT id, type, object_id, task_id FROM vault_bundle
            WHERE (type, object_id) IN %s""",
            (tuple(batch_bytes),),
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
            (row["type"], bytes(row["object_id"]))
            for row in bundles
            if row["task_id"] is None
        ]

        # Send the tasks
        args_batch = [
            (obj_type, hashutil.hash_to_hex(obj_id)) for obj_type, obj_id in batch_new
        ]
        # TODO: change once the scheduler handles priority tasks
        tasks = [
            create_oneshot_task_dict("swh-vault-batch-cooking", *args)
            for args in args_batch
        ]

        added_tasks = self.scheduler.create_tasks(tasks)
        tasks_ids_bundle_ids = [
            (task_id, obj_type, obj_id)
            for task_id, (obj_type, obj_id) in zip(
                [task["id"] for task in added_tasks], batch_new
            )
        ]

        # Update the task ids
        execute_values(
            cur,
            """
            UPDATE vault_bundle
            SET task_id = s_task_id
            FROM (VALUES %s) AS sub (s_task_id, s_type, s_object_id)
            WHERE type = s_type::cook_type AND object_id = s_object_id """,
            tasks_ids_bundle_ids,
        )
        return {"id": batch_id}

    @db_transaction()
    def batch_progress(self, batch_id: int, db=None, cur=None) -> Dict[str, Any]:
        cur.execute(
            """
            SELECT vault_bundle.id as id,
                   type, object_id, task_id, task_status, sticky,
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
            bundle["object_id"] = hashutil.hash_to_hex(bundle["object_id"])

        counter = collections.Counter(b["status"] for b in bundles)
        res = {
            "bundles": bundles,
            "total": len(bundles),
            **{k: 0 for k in ("new", "pending", "done", "failed")},
            **dict(counter),
        }

        return res

    @db_transaction()
    def is_available(self, obj_type: str, obj_id: ObjectId, db=None, cur=None):
        """Check whether a bundle is available for retrieval"""
        info = self.progress(obj_type, obj_id, raise_notfound=False, cur=cur)
        obj_id = hashutil.hash_to_bytes(obj_id)
        return (
            info is not None
            and info["task_status"] == "done"
            and self.cache.is_cached(obj_type, obj_id)
        )

    @db_transaction()
    def fetch(
        self, obj_type: str, obj_id: ObjectId, raise_notfound=True, db=None, cur=None
    ):
        """Retrieve a bundle from the cache"""
        hex_id, obj_id = self._compute_ids(obj_id)
        available = self.is_available(obj_type, obj_id, cur=cur)
        if not available:
            if raise_notfound:
                raise NotFoundExc(f"{obj_type} {hex_id} is not available.")
            return None
        self.update_access_ts(obj_type, obj_id, cur=cur)
        return self.cache.get(obj_type, obj_id)

    @db_transaction()
    def update_access_ts(self, obj_type: str, obj_id: bytes, db=None, cur=None):
        """Update the last access timestamp of a bundle"""
        cur.execute(
            """
            UPDATE vault_bundle
            SET ts_last_access = NOW()
            WHERE type = %s AND object_id = %s""",
            (obj_type, obj_id),
        )

    @db_transaction()
    def set_status(
        self, obj_type: str, obj_id: ObjectId, status: str, db=None, cur=None
    ) -> bool:
        obj_id = hashutil.hash_to_bytes(obj_id)
        req = (
            """
               UPDATE vault_bundle
               SET task_status = %s """
            + (""", ts_done = NOW() """ if status == "done" else "")
            + """WHERE type = %s AND object_id = %s"""
        )
        cur.execute(req, (status, obj_type, obj_id))
        return True

    @db_transaction()
    def set_progress(
        self, obj_type: str, obj_id: ObjectId, progress: str, db=None, cur=None
    ) -> bool:
        obj_id = hashutil.hash_to_bytes(obj_id)
        cur.execute(
            """
            UPDATE vault_bundle
            SET progress_msg = %s
            WHERE type = %s AND object_id = %s""",
            (progress, obj_type, obj_id),
        )
        return True

    @db_transaction()
    def send_notif(self, obj_type: str, obj_id: ObjectId, db=None, cur=None) -> bool:
        hex_id, obj_id = self._compute_ids(obj_id)
        cur.execute(
            """
            SELECT vault_notif_email.id AS id, email, task_status, progress_msg
            FROM vault_notif_email
            INNER JOIN vault_bundle ON bundle_id = vault_bundle.id
            WHERE vault_bundle.type = %s AND vault_bundle.object_id = %s""",
            (obj_type, obj_id),
        )
        for d in cur:
            self.send_notification(
                d["id"],
                d["email"],
                obj_type,
                hex_id,
                status=d["task_status"],
                progress_msg=d["progress_msg"],
            )
        return True

    @db_transaction()
    def send_notification(
        self,
        n_id: Optional[int],
        email: str,
        obj_type: str,
        hex_id: str,
        status: str,
        progress_msg: Optional[str] = None,
        db=None,
        cur=None,
    ) -> None:
        """Send the notification of a bundle to a specific e-mail"""
        short_id = hex_id[:7]

        # TODO: instead of hardcoding this, we should probably:
        # * add a "fetch_url" field in the vault_notif_email table
        # * generate the url with flask.url_for() on the web-ui side
        # * send this url as part of the cook request and store it in
        #   the table
        # * use this url for the notification e-mail
        url = "https://archive.softwareheritage.org/api/1/vault/{}/{}/" "raw".format(
            obj_type, hex_id
        )

        if status == "done":
            text = NOTIF_EMAIL_BODY_SUCCESS.strip()
            text = text.format(obj_type=obj_type, hex_id=hex_id, url=url)
            msg = MIMEText(text)
            msg["Subject"] = NOTIF_EMAIL_SUBJECT_SUCCESS.format(
                obj_type=obj_type, short_id=short_id
            )
        elif status == "failed":
            text = NOTIF_EMAIL_BODY_FAILURE.strip()
            text = text.format(
                obj_type=obj_type, hex_id=hex_id, progress_msg=progress_msg
            )
            msg = MIMEText(text)
            msg["Subject"] = NOTIF_EMAIL_SUBJECT_FAILURE.format(
                obj_type=obj_type, short_id=short_id
            )
        else:
            raise RuntimeError(
                "send_notification called on a '{}' bundle".format(status)
            )

        msg["From"] = NOTIF_EMAIL_FROM
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
        # Reconnect if needed
        try:
            status = self.smtp_server.noop()[0]
        except smtplib.SMTPException:
            status = -1
        if status != 250:
            self.smtp_server.connect("localhost", 25)

        # Send the message
        self.smtp_server.send_message(msg)

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
            RETURNING type, object_id
            """.format(
                cond
            ),
            args,
        )

        for d in cur:
            self.cache.delete(d["type"], bytes(d["object_id"]))

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
