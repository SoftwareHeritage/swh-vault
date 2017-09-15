# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import aiohttp.web
import click

from swh.core import config
from swh.core.api_async import (SWHRemoteAPI,
                                encode_data_server as encode_data)
from swh.model import hashutil
from swh.vault.cookers import COOKER_TYPES
from swh.vault.backend import VaultBackend


DEFAULT_CONFIG = {
    'storage': ('dict', {
        'cls': 'local',
        'args': {
            'db': 'dbname=softwareheritage-dev',
            'objstorage': {
                'root': '/srv/softwareheritage/objects',
                'slicing': '0:2/2:4/4:6',
            },
        },
    }),
    'cache': ('dict', {
        'cls': 'pathslicing',
        'args': {
            'root': '/srv/softwareheritage/vault',
            'slicing': '0:1/1:5',
        },
    }),
    'db': ('str', 'dbname=swh-vault')
}


@asyncio.coroutine
def index(request):
    return aiohttp.web.Response(body="SWH Vault API server")


@asyncio.coroutine
def vault_fetch(request):
    obj_type = request.match_info['type']
    obj_id = request.match_info['id']

    if not request.app['backend'].is_available(obj_type, obj_id):
        raise aiohttp.web.HTTPNotFound

    return encode_data(request.app['backend'].fetch(obj_type, obj_id))


def user_info(task_info):
    return {'task_uuid': str(task_info['task_uuid']),
            'status': task_info['task_status'],
            'progress_message': task_info['progress_msg'],
            'obj_type': task_info['type'],
            'obj_id': hashutil.hash_to_hex(task_info['object_id'])}


@asyncio.coroutine
def vault_cook(request):
    obj_type = request.match_info['type']
    obj_id = request.match_info['id']
    email = request.query.get('email')

    if obj_type not in COOKER_TYPES:
        raise aiohttp.web.HTTPNotFound

    info = request.app['backend'].cook_request(obj_type, obj_id, email)

    return encode_data(user_info(info), status=201)


@asyncio.coroutine
def vault_progress(request):
    obj_type = request.match_info['type']
    obj_id = request.match_info['id']

    info = request.app['backend'].task_info(obj_type, obj_id)
    if not info:
        raise aiohttp.web.HTTPNotFound

    return encode_data(user_info(info))


def make_app(config, **kwargs):
    app = SWHRemoteAPI(**kwargs)
    app.router.add_route('GET', '/', index)
    app.router.add_route('GET', '/fetch/{type}/{id}', vault_fetch)
    app.router.add_route('POST', '/cook/{type}/{id}', vault_cook)
    app.router.add_route('GET', '/progress/{type}/{id}', vault_progress)
    app['backend'] = VaultBackend(config)
    return app


@click.command()
@click.argument('config-path', required=1)
@click.option('--host', default='0.0.0.0', help="Host to run the server")
@click.option('--port', default=5005, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=True,
              help="Indicates if the server should run in debug mode")
def launch(config_path, host, port, debug):
    app = make_app(config.read(config_path, DEFAULT_CONFIG), debug=bool(debug))
    aiohttp.web.run_app(app, host=host, port=int(port))


if __name__ == '__main__':
    launch()
