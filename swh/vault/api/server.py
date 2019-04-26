# Copyright (C) 2016-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import aiohttp.web
import asyncio
import collections

from swh.core import config
from swh.core.api.asynchronous import (
    SWHRemoteAPI,
    encode_data_server as encode_data,
    decode_request,
)
from swh.model import hashutil
from swh.vault import get_vault
from swh.vault.cookers import COOKER_TYPES
from swh.vault.backend import NotFoundExc


DEFAULT_CONFIG_PATH = 'vault/server'
DEFAULT_CONFIG = {
    'storage': ('dict', {
        'cls': 'remote',
        'args': {
            'url': 'http://localhost:5002/',
        },
    }),
    'cache': ('dict', {
        'cls': 'pathslicing',
        'args': {
            'root': '/srv/softwareheritage/vault',
            'slicing': '0:1/1:5',
        },
    }),
    'client_max_size': ('int', 1024 ** 3),
    'vault': ('dict', {
        'cls': 'local',
        'args': {
            'db': 'dbname=softwareheritage-vault-dev',
        },
    }),
    'scheduler': ('dict', {
        'cls': 'remote',
        'args': {
            'url': 'http://localhost:5008/',
        }
    }),
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


# Batch endpoints

@asyncio.coroutine
def batch_cook(request):
    batch = yield from decode_request(request)
    for obj_type, obj_id in batch:
        if obj_type not in COOKER_TYPES:
            raise aiohttp.web.HTTPNotFound
    batch_id = request.app['backend'].batch_cook(batch)
    return encode_data({'id': batch_id})


@asyncio.coroutine
def batch_progress(request):
    batch_id = request.match_info['batch_id']
    bundles = request.app['backend'].batch_info(batch_id)
    if not bundles:
        raise aiohttp.web.HTTPNotFound
    bundles = [user_info(bundle) for bundle in bundles]
    counter = collections.Counter(b['status'] for b in bundles)
    res = {'bundles': bundles, 'total': len(bundles),
           **{k: 0 for k in ('new', 'pending', 'done', 'failed')},
           **dict(counter)}
    return encode_data(res)


# Web server

def make_app(backend, **kwargs):
    app = SWHRemoteAPI(**kwargs)
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

    # Endpoints for batch requests
    app.router.add_route('POST', '/batch_cook', batch_cook)
    app.router.add_route('GET', '/batch_progress/{batch_id}', batch_progress)

    app['backend'] = backend
    return app


def get_local_backend(config_file):
    if os.path.isfile(config_file):
        cfg = config.read(config_file, DEFAULT_CONFIG)
    else:
        cfg = config.load_named_config(config_file, DEFAULT_CONFIG)

    if 'vault' not in cfg:
        raise ValueError("missing '%vault' configuration")

    vcfg = cfg['vault']
    if vcfg['cls'] != 'local':
        raise EnvironmentError(
            "The vault backend can only be started with a 'local' "
            "configuration", err=True)
    args = vcfg['args']
    if 'cache' not in args:
        args['cache'] = cfg.get('cache')
    if 'storage' not in args:
        args['storage'] = cfg.get('storage')
    if 'scheduler' not in args:
        args['scheduler'] = cfg.get('scheduler')

    for key in ('cache', 'storage', 'scheduler'):
        if not args.get(key):
            raise ValueError(
                "invalid configuration; missing %s config entry." % key)

    return get_vault('local', args)


def make_app_from_configfile(config_file=DEFAULT_CONFIG_PATH, **kwargs):
    config_file = os.environ.get('SWH_CONFIG_FILENAME', config_file)
    vault = get_local_backend(config_file)
    return make_app(backend=vault, **kwargs)


if __name__ == '__main__':
    print('Deprecated. Use swh-vault ')
