# Copyright (C) 2016  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import asyncio
import aiohttp.web
import click

from swh.core import config
from swh.core.api_async import (SWHRemoteAPI,
                                encode_data_server as encode_data,
                                decode_request)
from swh.model import hashutil
from swh.vault.cookers import COOKER_TYPES
from swh.vault.backend import VaultBackend, NotFoundExc


DEFAULT_CONFIG_PATH = 'vault/server'
DEFAULT_CONFIG = {
    'storage': ('dict', {
        'cls': 'local',
        'args': {
            'db': 'dbname=softwareheritage-dev',
            'objstorage': {
                'cls': 'pathslicing',
                'args': {
                    'root': '/srv/softwareheritage/objects',
                    'slicing': '0:2/2:4/4:6',
                },
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
    'db': ('str', 'dbname=softwareheritage-vault-dev'),
    'scheduling_db': ('str', 'dbname=softwareheritage-scheduler-dev'),
}


@asyncio.coroutine
def index(request):
    return aiohttp.web.Response(body="SWH Vault API server")


# Web API endpoints

@asyncio.coroutine
def vault_fetch(request):
    obj_type = request.match_info['type']
    obj_id = request.match_info['id']

    if not request.app['backend'].is_available(obj_type, obj_id):
        raise aiohttp.web.HTTPNotFound

    return encode_data(request.app['backend'].fetch(obj_type, obj_id))


def user_info(task_info):
    return {'id': task_info['id'],
            'status': task_info['task_status'],
            'progress_message': task_info['progress_msg'],
            'obj_type': task_info['type'],
            'obj_id': hashutil.hash_to_hex(task_info['object_id'])}


@asyncio.coroutine
def vault_cook(request):
    obj_type = request.match_info['type']
    obj_id = request.match_info['id']
    email = request.query.get('email')
    sticky = request.query.get('sticky') in ('true', '1')

    if obj_type not in COOKER_TYPES:
        raise aiohttp.web.HTTPNotFound

    try:
        info = request.app['backend'].cook_request(obj_type, obj_id,
                                                   email=email, sticky=sticky)
    except NotFoundExc:
        raise aiohttp.web.HTTPNotFound

    # TODO: return 201 status (Created) once the api supports it
    return encode_data(user_info(info))


@asyncio.coroutine
def vault_progress(request):
    obj_type = request.match_info['type']
    obj_id = request.match_info['id']

    info = request.app['backend'].task_info(obj_type, obj_id)
    if not info:
        raise aiohttp.web.HTTPNotFound

    return encode_data(user_info(info))


# Cookers endpoints

@asyncio.coroutine
def set_progress(request):
    obj_type = request.match_info['type']
    obj_id = request.match_info['id']
    progress = yield from decode_request(request)
    request.app['backend'].set_progress(obj_type, obj_id, progress)
    return encode_data(True)  # FIXME: success value?


@asyncio.coroutine
def set_status(request):
    obj_type = request.match_info['type']
    obj_id = request.match_info['id']
    status = yield from decode_request(request)
    request.app['backend'].set_status(obj_type, obj_id, status)
    return encode_data(True)  # FIXME: success value?


@asyncio.coroutine
def put_bundle(request):
    obj_type = request.match_info['type']
    obj_id = request.match_info['id']

    # TODO: handle streaming properly
    content = yield from decode_request(request)
    request.app['backend'].cache.add(obj_type, obj_id, content)
    return encode_data(True)  # FIXME: success value?


@asyncio.coroutine
def send_notif(request):
    obj_type = request.match_info['type']
    obj_id = request.match_info['id']
    request.app['backend'].send_all_notifications(obj_type, obj_id)
    return encode_data(True)  # FIXME: success value?


# Web server

def make_app(config, **kwargs):
    app = SWHRemoteAPI(client_max_size=2 ** 29, **kwargs)
    app.router.add_route('GET', '/', index)

    # Endpoints used by the web API
    app.router.add_route('GET', '/fetch/{type}/{id}', vault_fetch)
    app.router.add_route('POST', '/cook/{type}/{id}', vault_cook)
    app.router.add_route('GET', '/progress/{type}/{id}', vault_progress)

    # Endpoints used by the Cookers
    app.router.add_route('POST', '/set_progress/{type}/{id}', set_progress)
    app.router.add_route('POST', '/set_status/{type}/{id}', set_status)
    app.router.add_route('POST', '/put_bundle/{type}/{id}', put_bundle)
    app.router.add_route('POST', '/send_notif/{type}/{id}', send_notif)

    app['backend'] = VaultBackend(config)
    return app


def make_app_from_configfile(config_path=DEFAULT_CONFIG_PATH, **kwargs):
    cfg = config.load_named_config(config_path, DEFAULT_CONFIG)
    return make_app(cfg, **kwargs)


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
