# Copyright (C) 2020-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import os
from typing import Any, Dict

import pytest
import yaml

from swh.core.api.serializers import json_dumps, msgpack_dumps, msgpack_loads
from swh.vault.api.serializers import ENCODERS
import swh.vault.api.server
from swh.vault.api.server import app, check_config, get_vault, make_app_from_configfile
from swh.vault.tests.test_backend import TEST_SWHID


@pytest.fixture
def swh_vault_server_config(swh_vault_config: Dict[str, Any]) -> Dict[str, Any]:
    """Returns a vault server configuration, with ``storage``, ``scheduler`` and
    ``cache`` set at the toplevel"""
    return {
        "vault": {"cls": "postgresql", "db": swh_vault_config["db"]},
        "client_max_size": 1024**3,
        **{k: v for k, v in swh_vault_config.items() if k != "db"},
    }


@pytest.fixture
def swh_vault_server_config_file(swh_vault_server_config, monkeypatch, tmp_path):
    """Creates a vault server configuration file and sets it into SWH_CONFIG_FILENAME"""
    conf_path = os.path.join(str(tmp_path), "vault-server.yml")
    with open(conf_path, "w") as f:
        f.write(yaml.dump(swh_vault_server_config))
    monkeypatch.setenv("SWH_CONFIG_FILENAME", conf_path)
    return conf_path


def test_make_app_from_file_missing(monkeypatch):
    monkeypatch.delenv("SWH_CONFIG_FILENAME", raising=False)
    with pytest.raises(ValueError, match="Missing configuration path."):
        make_app_from_configfile()


def test_make_app_from_file_does_not_exist(tmp_path, monkeypatch):
    monkeypatch.delenv("SWH_CONFIG_FILENAME", raising=False)
    conf_path = os.path.join(str(tmp_path), "vault-server.yml")
    assert os.path.exists(conf_path) is False

    with pytest.raises(
        ValueError, match=f"Configuration path {conf_path} should exist."
    ):
        make_app_from_configfile(conf_path)


def test_make_app_from_env_variable(swh_vault_server_config_file):
    """Server initialization happens through env variable when no path is provided"""
    app = make_app_from_configfile()
    assert app is not None
    assert get_vault() is not None

    # Cleanup app
    del app.config["vault"]
    swh.vault.api.server.vault = None


def test_make_app_from_file(swh_vault_server_config, tmp_path, monkeypatch):
    """Server initialization happens through path if provided"""
    monkeypatch.delenv("SWH_CONFIG_FILENAME", raising=False)
    conf_path = os.path.join(str(tmp_path), "vault-server.yml")
    with open(conf_path, "w") as f:
        f.write(yaml.dump(swh_vault_server_config))

    app = make_app_from_configfile(conf_path)
    assert app is not None
    assert get_vault() is not None

    # Cleanup app
    del app.config["vault"]
    swh.vault.api.server.vault = None


@pytest.fixture
def vault_app(swh_vault_server_config_file):
    yield make_app_from_configfile()

    # Cleanup app
    del app.config["vault"]
    swh.vault.api.server.vault = None


@pytest.fixture
def cli(vault_app):
    cli = vault_app.test_client()
    return cli


def test_client_index(cli):
    resp = cli.get("/")
    assert resp.status == "200 OK"


def test_client_cook_notfound(cli):
    resp = cli.post(
        "/cook",
        data=json_dumps(
            {"bundle_type": "flat", "swhid": TEST_SWHID}, extra_encoders=ENCODERS
        ),
        headers=[("Content-Type", "application/json")],
    )
    assert resp.status == "400 BAD REQUEST"
    content = msgpack_loads(resp.data)
    assert content["type"] == "NotFoundExc"
    assert content["args"] == [f"flat {TEST_SWHID} was not found."]


def test_client_progress_notfound(cli):
    resp = cli.post(
        "/progress",
        data=json_dumps(
            {"bundle_type": "flat", "swhid": TEST_SWHID}, extra_encoders=ENCODERS
        ),
        headers=[("Content-Type", "application/json")],
    )
    assert resp.status == "400 BAD REQUEST"
    content = msgpack_loads(resp.data)
    assert content["type"] == "NotFoundExc"
    assert content["args"] == [f"flat {TEST_SWHID} was not found."]


def test_client_batch_cook_invalid_type(cli):
    resp = cli.post(
        "/batch_cook",
        data=msgpack_dumps({"batch": [("foobar", [])]}),
        headers={"Content-Type": "application/x-msgpack"},
    )
    assert resp.status == "400 BAD REQUEST"
    content = msgpack_loads(resp.data)
    assert content["type"] == "NotFoundExc"
    assert content["args"] == ["foobar is an unknown type."]


def test_client_batch_progress_notfound(cli):
    resp = cli.post(
        "/batch_progress",
        data=msgpack_dumps({"batch_id": 1}),
        headers={"Content-Type": "application/x-msgpack"},
    )
    assert resp.status == "400 BAD REQUEST"
    content = msgpack_loads(resp.data)
    assert content["type"] == "NotFoundExc"
    assert content["args"] == ["Batch 1 does not exist."]


def test_check_config_missing_vault_configuration() -> None:
    """Irrelevant configuration file path raises"""
    with pytest.raises(ValueError, match="missing 'vault' configuration"):
        check_config({})


def test_check_config_not_local() -> None:
    """Wrong configuration raises"""
    expected_error = (
        "The vault backend of a vault server cannot be a 'remote' configuration"
    )
    with pytest.raises(EnvironmentError, match=expected_error):
        check_config({"vault": {"cls": "remote"}})


def test_check_config_ok(swh_vault_server_config) -> None:
    """Check that the default config is accepted"""
    assert swh_vault_server_config["vault"]["cls"] == "postgresql"
    assert check_config(swh_vault_server_config) is not None


@pytest.mark.parametrize("missing_key", ["storage", "cache", "scheduler"])
def test_check_config_missing_key(missing_key, swh_vault_server_config) -> None:
    """Check that configs with a missing key get rejected"""
    config_ok = swh_vault_server_config
    config_ko = copy.deepcopy(config_ok)
    config_ko["vault"].pop(missing_key, None)
    config_ko.pop(missing_key, None)

    expected_error = f"invalid configuration: missing {missing_key} config entry"
    with pytest.raises(ValueError, match=expected_error):
        check_config(config_ko)
