# Copyright (C) 2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.vault import get_vault
from swh.vault.api.client import RemoteVaultClient
from swh.vault.backend import VaultBackend

SERVER_IMPLEMENTATIONS = [
    ("remote", RemoteVaultClient, {"url": "localhost"}),
    (
        "local",
        VaultBackend,
        {
            "db": "something",
            "cache": {"cls": "memory", "args": {}},
            "storage": {"cls": "remote", "url": "mock://storage-url"},
            "scheduler": {"cls": "remote", "url": "mock://scheduler-url"},
        },
    ),
]


@pytest.fixture
def mock_psycopg2(mocker):
    mocker.patch("swh.vault.backend.psycopg2.pool")
    mocker.patch("swh.vault.backend.psycopg2.extras")


def test_init_get_vault_failure():
    with pytest.raises(ValueError, match="Unknown Vault class"):
        get_vault("unknown-vault-storage")


@pytest.mark.parametrize("class_name,expected_class,kwargs", SERVER_IMPLEMENTATIONS)
def test_init_get_vault(class_name, expected_class, kwargs, mock_psycopg2):
    concrete_vault = get_vault(class_name, **kwargs)
    assert isinstance(concrete_vault, expected_class)


@pytest.mark.parametrize("class_name,expected_class,kwargs", SERVER_IMPLEMENTATIONS)
def test_init_get_vault_deprecation_warning(
    class_name, expected_class, kwargs, mock_psycopg2
):
    with pytest.warns(DeprecationWarning):
        concrete_vault = get_vault(class_name, args=kwargs)
    assert isinstance(concrete_vault, expected_class)


def test_init_get_vault_ok(swh_vault_config):
    concrete_vault = get_vault("local", **swh_vault_config)
    assert isinstance(concrete_vault, VaultBackend)
