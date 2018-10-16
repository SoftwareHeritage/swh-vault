# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import tempfile

from swh.model import hashutil
from swh.vault.backend import VaultBackend

from swh.storage.tests.storage_testing import StorageTestFixture
from swh.vault.tests import SQL_DIR


class VaultTestFixture(StorageTestFixture):
    """Mix this in a test subject class to get Vault Database testing support.

    This fixture requires to come before DbTestFixture and StorageTestFixture
    in the inheritance list as it uses their methods to setup its own internal
    components.

    Usage example:

        class TestVault(VaultTestFixture, unittest.TestCase):
            ...
    """
    TEST_DB_NAME = 'softwareheritage-test-vault'
    TEST_DB_DUMP = [StorageTestFixture.TEST_DB_DUMP,
                    os.path.join(SQL_DIR, '*.sql')]

    def setUp(self):
        super().setUp()
        self.cache_root = tempfile.TemporaryDirectory('vault-cache-')
        self.vault_config = {
            'storage': self.storage_config,
            'cache': {
                'cls': 'pathslicing',
                'args': {
                    'root': self.cache_root.name,
                    'slicing': '0:1/1:5',
                    'allow_delete': True,
                }
            },
            'db': 'postgresql:///' + self.TEST_DB_NAME,
            'scheduler': None,
        }
        self.vault_backend = VaultBackend(self.vault_config)

    def tearDown(self):
        self.cache_root.cleanup()
        self.vault_backend.close()
        self.reset_storage_tables()
        self.reset_vault_tables()
        super().tearDown()

    def reset_vault_tables(self):
        excluded = {'dbversion'}
        self.reset_db_tables(self.TEST_DB_NAME, excluded=excluded)


def hash_content(content):
    obj_id = hashutil.hash_data(content)['sha1']
    return content, obj_id
