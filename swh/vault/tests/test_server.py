# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.core.api.serializers import msgpack_loads, msgpack_dumps

from swh.vault.api.server import make_app


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
