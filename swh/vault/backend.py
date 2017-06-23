# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import textwrap
import smtplib
import celery
import psycopg2
import psycopg2.extras

from functools import wraps
from email.mime.text import MIMEText

from swh.model import hashutil
from swh.scheduler.utils import get_task
from swh.vault.cache import VaultCache
from swh.vault.cookers import COOKER_TYPES
from swh.vault.cooking_tasks import SWHCookingTask  # noqa

cooking_task_name = 'swh.vault.cooking_tasks.SWHCookingTask'


# TODO: Imported from swh.scheduler.backend. Factorization needed.
def autocommit(fn):
    @wraps(fn)
    def wrapped(self, *args, **kwargs):
        autocommit = False
        # TODO: I don't like using None, it's confusing for the user. how about
        # a NEW_CURSOR object()?
        if 'cursor' not in kwargs or not kwargs['cursor']:
            autocommit = True
            kwargs['cursor'] = self.cursor()

        try:
            ret = fn(self, *args, **kwargs)
        except:
            if autocommit:
                self.rollback()
            raise

        if autocommit:
            self.commit()

        return ret

    return wrapped


# TODO: This has to be factorized with other database base classes and helpers
# (swh.scheduler.backend.SchedulerBackend, swh.storage.db.BaseDb, ...)
# The three first methods are imported from swh.scheduler.backend.
class VaultBackend:
    """
    Backend for the Software Heritage vault.
    """
    def __init__(self, config):
        self.config = config
        self.cache = VaultCache(**self.config['cache'])
        self.db = None
        self.reconnect()
        self.smtp_server = smtplib.SMTP('localhost')

    def reconnect(self):
        if not self.db or self.db.closed:
            self.db = psycopg2.connect(
                dsn=self.config['vault_db'],
                cursor_factory=psycopg2.extras.RealDictCursor,
            )

    def cursor(self):
        """Return a fresh cursor on the database, with auto-reconnection in case
        of failure"""
        cur = None

        # Get a fresh cursor and reconnect at most three times
        tries = 0
        while True:
            tries += 1
            try:
                cur = self.db.cursor()
                cur.execute('select 1')
                break
            except psycopg2.OperationalError:
                if tries < 3:
                    self.reconnect()
                else:
                    raise
        return cur

    def commit(self):
        """Commit a transaction"""
        self.db.commit()

    def rollback(self):
        """Rollback a transaction"""
        self.db.rollback()

    @autocommit
    def task_info(self, obj_type, obj_id, cursor=None):
        obj_id = hashutil.hash_to_bytes(obj_id)
        cursor.execute('''
            SELECT id, type, object_id, task_uuid, task_status,
                   ts_created, ts_done, progress_msg
            FROM vault_bundle
            WHERE type = %s AND object_id = %s''', (obj_type, obj_id))
        return cursor.fetchone()

    @autocommit
    def create_task(self, obj_type, obj_id, cursor=None):
        obj_id = hashutil.hash_to_bytes(obj_id)
        assert obj_type in COOKER_TYPES

        task_uuid = celery.uuid()
        cursor.execute('''
            INSERT INTO vault_bundle (type, object_id, task_uuid)
            VALUES (%s, %s, %s)''', (obj_type, obj_id, task_uuid))

        args = [self.config, obj_type, obj_id]
        task = get_task(cooking_task_name)
        self.commit()
        task.apply_async(args, task_id=task_uuid)

    @autocommit
    def add_notif_email(self, obj_type, obj_id, email, cursor=None):
        obj_id = hashutil.hash_to_bytes(obj_id)
        cursor.execute('''
            INSERT INTO vault_notif_email (email, bundle_id)
            VALUES (%s, (SELECT id FROM vault_bundle
                         WHERE type = %s AND object_id = %s))''',
                       (email, obj_type, obj_id))

    @autocommit
    def cook_request(self, obj_type, obj_id, email=None, cursor=None):
        info = self.task_info(obj_type, obj_id)
        if info is None:
            self.create_task(obj_type, obj_id)
        if email is not None:
            if info is not None and info['task_status'] == 'done':
                self.send_notification(None, email, obj_type, obj_id)
            else:
                self.add_notif_email(obj_type, obj_id, email)

    @autocommit
    def is_available(self, obj_type, obj_id, cursor=None):
        info = self.task_info(obj_type, obj_id, cursor=cursor)
        return (info is not None
                and info['task_status'] == 'done'
                and self.cache.is_cached(obj_type, obj_id))

    @autocommit
    def fetch(self, obj_type, obj_id, cursor=None):
        if not self.is_available(obj_type, obj_id, cursor=cursor):
            return None
        self.update_access_ts(obj_type, obj_id, cursor=cursor)
        return self.cache.get(obj_type, obj_id)

    @autocommit
    def update_access_ts(self, obj_type, obj_id, cursor=None):
        obj_id = hashutil.hash_to_bytes(obj_id)
        cursor.execute('''
            UPDATE vault_bundle
            SET ts_last_access = NOW()
            WHERE type = %s AND object_id = %s''',
                       (obj_type, obj_id))

    @autocommit
    def set_status(self, obj_type, obj_id, status, cursor=None):
        obj_id = hashutil.hash_to_bytes(obj_id)
        req = ('''
               UPDATE vault_bundle
               SET task_status = %s '''
               + (''', ts_done = NOW() ''' if status == 'done' else '')
               + '''WHERE type = %s AND object_id = %s''')
        cursor.execute(req, (status, obj_type, obj_id))

    @autocommit
    def set_progress(self, obj_type, obj_id, progress, cursor=None):
        obj_id = hashutil.hash_to_bytes(obj_id)
        cursor.execute('''
            UPDATE vault_bundle
            SET progress_msg = %s
            WHERE type = %s AND object_id = %s''',
                       (progress, obj_type, obj_id))

    @autocommit
    def send_all_notifications(self, obj_type, obj_id, cursor=None):
        obj_id = hashutil.hash_to_bytes(obj_id)
        cursor.execute('''
            SELECT vault_notif_email.id AS id, email
            FROM vault_notif_email
            RIGHT JOIN vault_bundle ON bundle_id = vault_bundle.id
            WHERE vault_bundle.type = %s AND vault_bundle.object_id = %s''',
                       (obj_type, obj_id))
        for d in cursor:
            self.send_notification(d['id'], d['email'], obj_type, obj_id)

    @autocommit
    def send_notification(self, n_id, email, obj_type, obj_id, cursor=None):
        hex_id = hashutil.hash_to_hex(obj_id)
        text = (
            "You have requested a bundle of type `{obj_type}` for the object "
            "`{hex_id}` from the Software Heritage Archive.\n\n"
            "The bundle you requested is now available for download at the "
            "following address:\n\n"
            "{url}\n\n"
            "Please keep in mind that this link might expire at some point, "
            "in which case you will need to request the bundle again.")

        text = text.format(obj_type=obj_type, hex_id=hex_id, url='URL_TODO')
        text = textwrap.dedent(text)
        text = '\n'.join(textwrap.wrap(text, 72, replace_whitespace=False))
        msg = MIMEText(text)
        msg['Subject'] = ("The `{obj_type}` bundle of `{hex_id}` is ready"
                          .format(obj_type=obj_type, hex_id=hex_id))
        msg['From'] = '"Software Heritage Vault" <vault@softwareheritage.org>'
        msg['To'] = email

        self.smtp_server.send_message(msg)

        if n_id is not None:
            cursor.execute('''
                DELETE FROM vault_notif_email
                WHERE id = %s''', (n_id,))