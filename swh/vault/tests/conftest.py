# Copyright (C) 2020-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from functools import partial
import os
from typing import Any, Dict

import pytest
from pytest_postgresql import factories

from swh.core.db.db_utils import initialize_database_for_module
from swh.vault import get_vault
from swh.vault.backend import VaultBackend

os.environ["LC_ALL"] = "C.UTF-8"

# needed for directory tests on git-cloned repositories
# 022 is usually the default value, but some environments (eg. Debian builds) have
# a different one.
os.umask(0o022)


vault_postgresql_proc = factories.postgresql_proc(
    load=[
        partial(initialize_database_for_module, "vault", VaultBackend.current_version)
    ],
)

postgres_vault = factories.postgresql("vault_postgresql_proc")


def pytest_collection_modifyitems(items):
    """Skip tests using httpserver fixture if pytest-httpserver is
    not available (debian < 12 for instance)"""
    try:
        from pytest_httpserver import HTTPServer  # noqa
    except ImportError:
        pytest_httpserver_available = False
    else:
        pytest_httpserver_available = True
    for item in items:
        try:
            fixtures = item.fixturenames
            if "httpserver" in fixtures and not pytest_httpserver_available:
                item.add_marker(
                    pytest.mark.skip(reason="pytest-httpserver not installed")
                )
        except Exception:
            pass


@pytest.fixture
def swh_vault_config(postgres_vault, tmp_path) -> Dict[str, Any]:
    tmp_path = str(tmp_path)
    return {
        "db": postgres_vault.info.dsn,
        "storage": {
            "cls": "memory",
        },
        "cache": {
            "cls": "pathslicing",
            "root": tmp_path,
            "slicing": "0:1/1:5",
            "allow_delete": True,
        },
        "scheduler": {
            "cls": "remote",
            "url": "http://swh-scheduler:5008",
        },
    }


@pytest.fixture
def swh_vault(swh_vault_config):
    return get_vault("postgresql", **swh_vault_config)


@pytest.fixture
def swh_vault_custom_notif(swh_vault_config):
    notif_cfg = {
        "from": "Someone from somewhere <nobody@nowhere.local>",
        "api_url": "http://test.local/api/1",
    }
    return get_vault("postgresql", notification=notif_cfg, **swh_vault_config)


@pytest.fixture
def swh_storage(swh_vault):
    return swh_vault.storage
