# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import smtplib
import celery
import psycopg2
import psycopg2.extras

from functools import wraps
from email.mime.text import MIMEText

from swh.model import hashutil
from swh.scheduler.utils import get_task
from swh.vault.cache import VaultCache
from swh.vault.cookers import get_cooker
from swh.vault.cooking_tasks import SWHCookingTask  # noqa

cooking_task_name = 'swh.vault.cooking_tasks.SWHCookingTask'

NOTIF_EMAIL_FROM = ('"Software Heritage Vault" '
                    '<info@softwareheritage.org>')
NOTIF_EMAIL_SUBJECT = ("Bundle ready: {obj_type} {short_id}")
NOTIF_EMAIL_BODY = """
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
        """Reconnect to the database."""
        if not self.db or self.db.closed:
            self.db = psycopg2.connect(
                dsn=self.config['vault_db'],
                cursor_factory=psycopg2.extras.RealDictCursor,
            )

    def close(self):
        """Close the underlying database connection."""
        self.db.close()

    def cursor(self):
        """Return a fresh cursor on the database, with auto-reconnection in
        case of failure"""
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
        """Fetch information from a bundle"""
        obj_id = hashutil.hash_to_bytes(obj_id)
        cursor.execute('''
            SELECT id, type, object_id, task_uuid, task_status, sticky,
                   ts_created, ts_done, ts_last_access, progress_msg
            FROM vault_bundle
            WHERE type = %s AND object_id = %s''', (obj_type, obj_id))
        res = cursor.fetchone()
        if res:
            res['object_id'] = bytes(res['object_id'])
        return res

    def _send_task(task_uuid, args):
        """Send a cooking task to the celery scheduler"""
        task = get_task(cooking_task_name)
        task.apply_async(args, task_id=task_uuid)

    @autocommit
    def create_task(self, obj_type, obj_id, sticky=False, cursor=None):
        """Create and send a cooking task"""
        obj_id = hashutil.hash_to_bytes(obj_id)
        args = [self.config, obj_type, obj_id]
        CookerCls = get_cooker(obj_type)
        cooker = CookerCls(*args)
        cooker.check_exists()

        task_uuid = celery.uuid()
        cursor.execute('''
            INSERT INTO vault_bundle (type, object_id, task_uuid, sticky)
            VALUES (%s, %s, %s, %s)''',
                       (obj_type, obj_id, task_uuid, sticky))
        self.commit()

        self._send_task(task_uuid, args)

    @autocommit
    def add_notif_email(self, obj_type, obj_id, email, cursor=None):
        """Add an e-mail address to notify when a given bundle is ready"""
        obj_id = hashutil.hash_to_bytes(obj_id)
        cursor.execute('''
            INSERT INTO vault_notif_email (email, bundle_id)
            VALUES (%s, (SELECT id FROM vault_bundle
                         WHERE type = %s AND object_id = %s))''',
                       (email, obj_type, obj_id))

    @autocommit
    def cook_request(self, obj_type, obj_id, *, sticky=False,
                     email=None, cursor=None):
        """Main entry point for cooking requests. This starts a cooking task if
            needed, and add the given e-mail to the notify list"""
        info = self.task_info(obj_type, obj_id)
        if info is None:
            self.create_task(obj_type, obj_id, sticky)
        if email is not None:
            if info is not None and info['task_status'] == 'done':
                self.send_notification(None, email, obj_type, obj_id)
            else:
                self.add_notif_email(obj_type, obj_id, email)
        info = self.task_info(obj_type, obj_id)
        return info

    @autocommit
    def is_available(self, obj_type, obj_id, cursor=None):
        """Check whether a bundle is available for retrieval"""
        info = self.task_info(obj_type, obj_id, cursor=cursor)
        return (info is not None
                and info['task_status'] == 'done'
                and self.cache.is_cached(obj_type, obj_id))

    @autocommit
    def fetch(self, obj_type, obj_id, cursor=None):
        """Retrieve a bundle from the cache"""
        if not self.is_available(obj_type, obj_id, cursor=cursor):
            return None
        self.update_access_ts(obj_type, obj_id, cursor=cursor)
        return self.cache.get(obj_type, obj_id)

    @autocommit
    def update_access_ts(self, obj_type, obj_id, cursor=None):
        """Update the last access timestamp of a bundle"""
        obj_id = hashutil.hash_to_bytes(obj_id)
        cursor.execute('''
            UPDATE vault_bundle
            SET ts_last_access = NOW()
            WHERE type = %s AND object_id = %s''',
                       (obj_type, obj_id))

    @autocommit
    def set_status(self, obj_type, obj_id, status, cursor=None):
        """Set the cooking status of a bundle"""
        obj_id = hashutil.hash_to_bytes(obj_id)
        req = ('''
               UPDATE vault_bundle
               SET task_status = %s '''
               + (''', ts_done = NOW() ''' if status == 'done' else '')
               + '''WHERE type = %s AND object_id = %s''')
        cursor.execute(req, (status, obj_type, obj_id))

    @autocommit
    def set_progress(self, obj_type, obj_id, progress, cursor=None):
        """Set the cooking progress of a bundle"""
        obj_id = hashutil.hash_to_bytes(obj_id)
        cursor.execute('''
            UPDATE vault_bundle
            SET progress_msg = %s
            WHERE type = %s AND object_id = %s''',
                       (progress, obj_type, obj_id))

    @autocommit
    def send_all_notifications(self, obj_type, obj_id, cursor=None):
        """Send all the e-mails in the notification list of a bundle"""
        obj_id = hashutil.hash_to_bytes(obj_id)
        cursor.execute('''
            SELECT vault_notif_email.id AS id, email
            FROM vault_notif_email
            INNER JOIN vault_bundle ON bundle_id = vault_bundle.id
            WHERE vault_bundle.type = %s AND vault_bundle.object_id = %s''',
                       (obj_type, obj_id))
        for d in cursor:
            self.send_notification(d['id'], d['email'], obj_type, obj_id)

    @autocommit
    def send_notification(self, n_id, email, obj_type, obj_id, cursor=None):
        """Send the notification of a bundle to a specific e-mail"""
        hex_id = hashutil.hash_to_hex(obj_id)
        short_id = hex_id[:7]

        # TODO: instead of hardcoding this, we should probably:
        # * add a "fetch_url" field in the vault_notif_email table
        # * generate the url with flask.url_for() on the web-ui side
        # * send this url as part of the cook request and store it in
        #   the table
        # * use this url for the notification e-mail
        url = ('https://archive.softwareheritage.org/api/1/vault/{}/{}/'
               'raw'.format(obj_type, hex_id))

        text = NOTIF_EMAIL_BODY.strip()
        text = text.format(obj_type=obj_type, hex_id=hex_id, url=url)
        msg = MIMEText(text)
        msg['Subject'] = (NOTIF_EMAIL_SUBJECT
                          .format(obj_type=obj_type, short_id=short_id))
        msg['From'] = NOTIF_EMAIL_FROM
        msg['To'] = email

        self.smtp_server.send_message(msg)

        if n_id is not None:
            cursor.execute('''
                DELETE FROM vault_notif_email
                WHERE id = %s''', (n_id,))

    @autocommit
    def _cache_expire(self, cond, *args, cursor=None):
        """Low-level expiration method, used by cache_expire_* methods"""
        # Embedded SELECT query to be able to use ORDER BY and LIMIT
        cursor.execute('''
            DELETE FROM vault_bundle
            WHERE ctid IN (
                SELECT ctid
                FROM vault_bundle
                WHERE sticky = false
                {}
            )
            RETURNING type, object_id
            '''.format(cond), args)

        for d in cursor:
            self.cache.delete(d['type'], bytes(d['object_id']))

    @autocommit
    def cache_expire_oldest(self, n=1, by='last_access', cursor=None):
        """Expire the `n` oldest bundles"""
        assert by in ('created', 'done', 'last_access')
        filter = '''ORDER BY ts_{} LIMIT {}'''.format(by, n)
        return self._cache_expire(filter)

    @autocommit
    def cache_expire_until(self, date, by='last_access', cursor=None):
        """Expire all the bundles until a certain date"""
        assert by in ('created', 'done', 'last_access')
        filter = '''AND ts_{} <= %s'''.format(by)
        return self._cache_expire(filter, date)
