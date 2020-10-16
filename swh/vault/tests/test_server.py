# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import copy

import pytest

from swh.core.api.serializers import msgpack_dumps, msgpack_loads
from swh.vault.api.server import check_config, make_app


@pytest.fixture
def client(swh_vault, loop, aiohttp_client):
    app = make_app(backend=swh_vault)
    return loop.run_until_complete(aiohttp_client(app))


async def test_index(client):
    resp = await client.get("/")
    assert resp.status == 200


async def test_cook_notfound(client):
    resp = await client.post("/cook/directory/000000")
    assert resp.status == 400
    content = msgpack_loads(await resp.content.read())
    assert content["exception"]["type"] == "NotFoundExc"
    assert content["exception"]["args"] == ["Object 000000 was not found."]


async def test_progress_notfound(client):
    resp = await client.get("/progress/directory/000000")
    assert resp.status == 400
    content = msgpack_loads(await resp.content.read())
    assert content["exception"]["type"] == "NotFoundExc"
    assert content["exception"]["args"] == ["directory 000000 was not found."]


async def test_batch_cook_invalid_type(client):
    data = msgpack_dumps([("foobar", [])])
    resp = await client.post(
        "/batch_cook", data=data, headers={"Content-Type": "application/x-msgpack"}
    )
    assert resp.status == 400
    content = msgpack_loads(await resp.content.read())
    assert content["exception"]["type"] == "NotFoundExc"
    assert content["exception"]["args"] == ["foobar is an unknown type."]


async def test_batch_progress_notfound(client):
    resp = await client.get("/batch_progress/1")
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
    config_ok = {"vault": {"cls": "local", "args": swh_vault_config}}
    config_ko = copy.deepcopy(config_ok)
    config_ko["vault"]["args"].pop(missing_key, None)

    expected_error = f"invalid configuration: missing {missing_key} config entry"
    with pytest.raises(ValueError, match=expected_error):
        check_config(config_ko)


@pytest.mark.parametrize("missing_key", ["storage", "cache", "scheduler"])
def test_check_config_ok(missing_key, swh_vault_config) -> None:
    """Any other configuration than 'local' (the default) is rejected"""
    config_ok = {"vault": {"cls": "local", "args": swh_vault_config}}
    assert check_config(config_ok) is not None
