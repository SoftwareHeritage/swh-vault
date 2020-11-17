# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy
import os
from typing import Any, Dict

import pytest
import yaml

from swh.core.api.serializers import json_dumps, msgpack_dumps, msgpack_loads
from swh.vault.api.server import (
    VaultServerApp,
    check_config,
    make_app,
    make_app_from_configfile,
)
from swh.vault.tests.test_backend import TEST_HEX_ID


def test_make_app_from_file_missing():
    with pytest.raises(ValueError, match="Missing configuration path."):
        make_app_from_configfile()


def test_make_app_from_file_does_not_exist(tmp_path):
    conf_path = os.path.join(str(tmp_path), "vault-server.yml")
    assert os.path.exists(conf_path) is False

    with pytest.raises(
        ValueError, match=f"Configuration path {conf_path} should exist."
    ):
        make_app_from_configfile(conf_path)


def test_make_app_from_env_variable(swh_vault_config_file):
    """Server initialization happens through env variable when no path is provided

    """
    app = make_app_from_configfile()
    assert app is not None


def test_make_app_from_file(swh_local_vault_config, tmp_path):
    """Server initialization happens trough path if provided

    """
    conf_path = os.path.join(str(tmp_path), "vault-server.yml")
    with open(conf_path, "w") as f:
        f.write(yaml.dump(swh_local_vault_config))

    app = make_app_from_configfile(conf_path)
    assert app is not None


@pytest.fixture
def async_app(swh_local_vault_config: Dict[str, Any],) -> VaultServerApp:
    """Instantiate the vault server application.

    Note: This requires the db setup to run (fixture swh_vault in charge of this)

    """
    return make_app(swh_local_vault_config)


@pytest.fixture
def cli(async_app, aiohttp_client, loop):
    return loop.run_until_complete(aiohttp_client(async_app))


async def test_client_index(cli):
    resp = await cli.get("/")
    assert resp.status == 200


async def test_client_cook_notfound(cli):
    resp = await cli.post(
        "/cook",
        data=json_dumps({"obj_type": "directory", "obj_id": TEST_HEX_ID}),
        headers=[("Content-Type", "application/json")],
    )
    assert resp.status == 400
    content = msgpack_loads(await resp.content.read())
    assert content["exception"]["type"] == "NotFoundExc"
    assert content["exception"]["args"] == [f"directory {TEST_HEX_ID} was not found."]


async def test_client_progress_notfound(cli):
    resp = await cli.post(
        "/progress",
        data=json_dumps({"obj_type": "directory", "obj_id": TEST_HEX_ID}),
        headers=[("Content-Type", "application/json")],
    )
    assert resp.status == 400
    content = msgpack_loads(await resp.content.read())
    assert content["exception"]["type"] == "NotFoundExc"
    assert content["exception"]["args"] == [f"directory {TEST_HEX_ID} was not found."]


async def test_client_batch_cook_invalid_type(cli):
    resp = await cli.post(
        "/batch_cook",
        data=msgpack_dumps({"batch": [("foobar", [])]}),
        headers={"Content-Type": "application/x-msgpack"},
    )
    assert resp.status == 400
    content = msgpack_loads(await resp.content.read())
    assert content["exception"]["type"] == "NotFoundExc"
    assert content["exception"]["args"] == ["foobar is an unknown type."]


async def test_client_batch_progress_notfound(cli):
    resp = await cli.post(
        "/batch_progress",
        data=msgpack_dumps({"batch_id": 1}),
        headers={"Content-Type": "application/x-msgpack"},
    )
    assert resp.status == 400
    content = msgpack_loads(await resp.content.read())
    assert content["exception"]["type"] == "NotFoundExc"
    assert content["exception"]["args"] == ["Batch 1 does not exist."]


def test_check_config_missing_vault_configuration() -> None:
    """Irrelevant configuration file path raises"""
    with pytest.raises(ValueError, match="missing 'vault' configuration"):
        check_config({})


def test_check_config_not_local() -> None:
    """Wrong configuration raises"""
    expected_error = (
        "The vault backend can only be started with a 'local' configuration"
    )
    with pytest.raises(EnvironmentError, match=expected_error):
        check_config({"vault": {"cls": "remote"}})


@pytest.mark.parametrize("missing_key", ["storage", "cache", "scheduler"])
def test_check_config_missing_key(missing_key, swh_vault_config) -> None:
    """Any other configuration than 'local' (the default) is rejected"""
    config_ok = {"vault": {"cls": "local", **swh_vault_config}}
    config_ko = copy.deepcopy(config_ok)
    config_ko["vault"].pop(missing_key, None)

    expected_error = f"invalid configuration: missing {missing_key} config entry"
    with pytest.raises(ValueError, match=expected_error):
        check_config(config_ko)


@pytest.mark.parametrize("missing_key", ["storage", "cache", "scheduler"])
def test_check_config_ok(missing_key, swh_vault_config) -> None:
    """Any other configuration than 'local' (the default) is rejected"""
    config_ok = {"vault": {"cls": "local", **swh_vault_config}}
    assert check_config(config_ok) is not None
