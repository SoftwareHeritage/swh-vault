import pytest
import glob
import os
import pkg_resources.extern.packaging.version

from swh.core.utils import numfile_sortkey as sortkey
from swh.vault import get_vault
from swh.vault.tests import SQL_DIR
from swh.storage.tests import SQL_DIR as STORAGE_SQL_DIR
from pytest_postgresql import factories


os.environ['LC_ALL'] = 'C.UTF-8'

pytest_v = pkg_resources.get_distribution("pytest").parsed_version
if pytest_v < pkg_resources.extern.packaging.version.parse('3.9'):
    @pytest.fixture
    def tmp_path(request):
        import tempfile
        import pathlib
        with tempfile.TemporaryDirectory() as tmpdir:
            yield pathlib.Path(tmpdir)


def db_url(name, postgresql_proc):
    return 'postgresql://{user}@{host}:{port}/{dbname}'.format(
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        user='postgres',
        dbname=name)


postgresql2 = factories.postgresql('postgresql_proc', 'tests2')


@pytest.fixture
def swh_vault(request, postgresql_proc, postgresql, postgresql2, tmp_path):

    for sql_dir, pg in ((SQL_DIR, postgresql), (STORAGE_SQL_DIR, postgresql2)):
        dump_files = os.path.join(sql_dir, '*.sql')
        all_dump_files = sorted(glob.glob(dump_files), key=sortkey)

        cursor = pg.cursor()
        for fname in all_dump_files:
            with open(fname) as fobj:
                # disable concurrent index creation since we run in a
                # transaction
                cursor.execute(fobj.read().replace('concurrently', ''))
        pg.commit()

    vault_config = {
        'db': db_url('tests', postgresql_proc),
        'storage': {
            'cls': 'local',
            'args': {
                'db': db_url('tests2', postgresql_proc),
                'objstorage': {
                    'cls': 'pathslicing',
                    'args': {
                        'root': str(tmp_path),
                        'slicing': '0:1/1:5',
                    },
                },
            },
        },
        'cache': {
            'cls': 'pathslicing',
            'args': {
                'root': str(tmp_path),
                'slicing': '0:1/1:5',
                'allow_delete': True,
            }
        },
        'scheduler': {
            'cls': 'remote',
            'args': {
                'url': 'http://swh-scheduler:5008',
            },
        },
    }

    return get_vault('local', vault_config)
