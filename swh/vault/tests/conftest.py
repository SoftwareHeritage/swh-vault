# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Any, Dict

import pkg_resources.extern.packaging.version
import pytest
import yaml

from swh.core.db.pytest_plugin import postgresql_fact
from swh.storage.tests import SQL_DIR as STORAGE_SQL_DIR
import swh.vault
from swh.vault import get_vault

os.environ["LC_ALL"] = "C.UTF-8"

pytest_v = pkg_resources.get_distribution("pytest").parsed_version
if pytest_v < pkg_resources.extern.packaging.version.parse("3.9"):

    @pytest.fixture
    def tmp_path():
        import pathlib
        import tempfile

        with tempfile.TemporaryDirectory() as tmpdir:
            yield pathlib.Path(tmpdir)


def db_url(name, postgresql_proc):
    return "postgresql://{user}@{host}:{port}/{dbname}".format(
        host=postgresql_proc.host,
        port=postgresql_proc.port,
        user="postgres",
        dbname=name,
    )


VAULT_SQL_DIR = os.path.join(os.path.dirname(swh.vault.__file__), "sql")


postgres_vault = postgresql_fact(
    "postgresql_proc", db_name="vault", dump_files=f"{VAULT_SQL_DIR}/*.sql"
)
postgres_storage = postgresql_fact(
    "postgresql_proc", db_name="storage", dump_files=f"{STORAGE_SQL_DIR}/*.sql"
)


@pytest.fixture
def swh_vault_config(postgres_vault, postgres_storage, tmp_path) -> Dict[str, Any]:
    tmp_path = str(tmp_path)
    return {
        "db": postgres_vault.dsn,
        "storage": {
            "cls": "local",
            "db": postgres_storage.dsn,
            "objstorage": {
                "cls": "pathslicing",
                "args": {"root": tmp_path, "slicing": "0:1/1:5",},
            },
        },
        "cache": {
            "cls": "pathslicing",
            "args": {"root": tmp_path, "slicing": "0:1/1:5", "allow_delete": True},
        },
        "scheduler": {"cls": "remote", "url": "http://swh-scheduler:5008",},
    }


@pytest.fixture
def swh_local_vault_config(swh_vault_config: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "vault": {"cls": "local", **swh_vault_config},
        "client_max_size": 1024 ** 3,
    }


@pytest.fixture
def swh_vault_config_file(swh_local_vault_config, monkeypatch, tmp_path):
    conf_path = os.path.join(str(tmp_path), "vault-server.yml")
    with open(conf_path, "w") as f:
        f.write(yaml.dump(swh_local_vault_config))
    monkeypatch.setenv("SWH_CONFIG_FILENAME", conf_path)
    return conf_path


@pytest.fixture
def swh_vault(swh_vault_config):
    return get_vault("local", **swh_vault_config)


@pytest.fixture
def swh_storage(swh_vault):
    return swh_vault.storage
