# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
import pathlib

from swh.model import hashutil
from swh.vault.backend import VaultBackend


class VaultTestFixture:
    """Mix this in a test subject class to get Vault Database testing support.

    This fixture requires to come before DbTestFixture and StorageTestFixture
    in the inheritance list as it uses their methods to setup its own internal
    components.

    Usage example:

        class TestVault(VaultTestFixture, StorageTestFixture, DbTestFixture):
            ...
    """
    TEST_VAULT_DB_NAME = 'softwareheritage-test-vault'

    @classmethod
    def setUpClass(cls):
        if not hasattr(cls, 'DB_TEST_FIXTURE_IMPORTED'):
            raise RuntimeError("VaultTestFixture needs to be followed by "
                               "DbTestFixture in the inheritance list.")

        test_dir = pathlib.Path(__file__).absolute().parent
        test_db_dump = test_dir / '../../../sql/swh-vault-schema.sql'
        test_db_dump = test_db_dump.absolute()
        cls.add_db(cls.TEST_VAULT_DB_NAME, str(test_db_dump), 'psql')
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.cache_root = tempfile.TemporaryDirectory('vault-cache-')
        self.vault_config = {
            'storage': self.storage_config,
            'db': 'postgresql:///' + self.TEST_VAULT_DB_NAME,
            'cache': {
                'cls': 'pathslicing',
                'args': {
                    'root': self.cache_root.name,
                    'slicing': '0:1/1:5',
                    'allow_delete': True,
                }
            }
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
        self.reset_db_tables(self.TEST_VAULT_DB_NAME, excluded=excluded)


def hash_content(content):
    obj_id = hashutil.hash_data(content)['sha1']
    return content, obj_id
