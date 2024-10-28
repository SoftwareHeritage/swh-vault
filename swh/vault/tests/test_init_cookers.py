# Copyright (C) 2017-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
from typing import Dict

import pytest
import yaml

from swh.vault.cookers import COOKER_TYPES, get_cooker
from swh.vault.tests.test_backend import TEST_SWHID


@pytest.fixture
def swh_cooker_config():
    return {
        "vault": {
            "cls": "remote",
            "url": "mock://vault-backend",
            "storage": {"cls": "remote", "url": "mock://storage-url"},
        }
    }


def write_config_to_env(config: Dict, tmp_path, monkeypatch) -> str:
    """Write the configuration dict into a temporary file, then reference that path to
    SWH_CONFIG_FILENAME environment variable.

    """
    conf_path = os.path.join(str(tmp_path), "cooker.yml")
    with open(conf_path, "w") as f:
        f.write(yaml.dump(config))
    monkeypatch.setenv("SWH_CONFIG_FILENAME", conf_path)
    return conf_path


def test_write_to_env(swh_cooker_config, tmp_path, monkeypatch):
    actual_path = write_config_to_env(swh_cooker_config, tmp_path, monkeypatch)

    assert os.path.exists(actual_path) is True
    assert os.environ["SWH_CONFIG_FILENAME"] == actual_path

    with open(actual_path, "r") as f:
        actual_config = yaml.safe_load(f.read())
    assert actual_config == swh_cooker_config


@pytest.mark.parametrize(
    "config_ko,exception_class,exception_msg",
    [
        ({}, ValueError, "missing 'vault' configuration"),
        (
            {"vault": {"cls": "postgresql"}},
            EnvironmentError,
            "This vault backend can only be a 'remote' configuration",
        ),
        (
            {"vault": {"cls": "remote", "missing-storage-key": ""}},
            ValueError,
            "invalid configuration: missing 'storage' config entry",
        ),
    ],
)
def test_get_cooker_config_ko(
    config_ko, exception_class, exception_msg, monkeypatch, tmp_path
):
    """Misconfigured cooker should fail the instantiation with exception message"""
    write_config_to_env(config_ko, tmp_path, monkeypatch)

    with pytest.raises(exception_class, match=exception_msg):
        get_cooker("flat", TEST_SWHID)


@pytest.mark.parametrize(
    "config_ok",
    [
        {
            "vault": {
                "cls": "remote",
                "url": "mock://vault-backend",
                "storage": {"cls": "remote", "url": "mock://storage-url"},
            }
        },
        {
            "vault": {
                "cls": "remote",
                "url": "mock://vault-backend",
            },
            "storage": {"cls": "remote", "url": "mock://storage-url"},
        },
        {
            "vault": {
                "cls": "remote",
                "url": "mock://vault-backend",
            },
            "storage": {"cls": "remote", "url": "mock://storage-url"},
            "objstorage": {"cls": "memory"},
        },
        {
            "vault": {
                "cls": "remote",
                "url": "mock://vault-backend",
            },
            "storage": {"cls": "remote", "url": "mock://storage-url"},
            "graph": {"url": "mock://graph-url"},
        },
    ],
)
def test_get_cooker_nominal(config_ok, tmp_path, monkeypatch, requests_mock):
    """Correct configuration should allow the instantiation of the cookers"""
    requests_mock.get(
        "mock://graph-url/stats",
        json={"num_nodes": 42},
        headers={"Content-Type": "application/json"},
    )

    for cooker_type in COOKER_TYPES.keys():
        write_config_to_env(config_ok, tmp_path, monkeypatch)

        cooker = get_cooker(cooker_type, TEST_SWHID)

        assert cooker is not None
        assert isinstance(cooker, tuple(COOKER_TYPES[cooker_type]))
        if config_ok.get("objstorage") or config_ok["vault"].get("objstorage"):
            assert cooker.objstorage is not None
        else:
            assert cooker.objstorage is None
        if config_ok.get("graph") or config_ok["vault"].get("graph"):
            assert cooker.graph is not None
        else:
            assert cooker.graph is None
