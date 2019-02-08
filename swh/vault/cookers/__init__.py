# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information
import os

from swh.core.config import load_named_config, read as read_config
from swh.storage import get_storage
from swh.vault import get_vault
from swh.vault.cookers.base import DEFAULT_CONFIG_PATH, DEFAULT_CONFIG
from swh.vault.cookers.directory import DirectoryCooker
from swh.vault.cookers.revision_flat import RevisionFlatCooker
from swh.vault.cookers.revision_gitfast import RevisionGitfastCooker

COOKER_TYPES = {
    'directory': DirectoryCooker,
    'revision_flat': RevisionFlatCooker,
    'revision_gitfast': RevisionGitfastCooker,
}


def get_cooker_cls(obj_type):
    return COOKER_TYPES[obj_type]


def get_cooker(obj_type, obj_id):
    if 'SWH_CONFIG_FILENAME' in os.environ:
        cfg = read_config(os.environ['SWH_CONFIG_FILENAME'], DEFAULT_CONFIG)
    else:
        cfg = load_named_config(DEFAULT_CONFIG_PATH, DEFAULT_CONFIG)
    cooker_cls = get_cooker_cls(obj_type)
    if 'vault' not in cfg:
        raise ValueError("missing '%vault' configuration")

    vcfg = cfg['vault']
    if vcfg['cls'] != 'remote':
        raise EnvironmentError(
            "This vault backend can only be a 'remote' "
            "configuration", err=True)
    args = vcfg['args']
    if 'storage' not in args:
        args['storage'] = cfg.get('storage')

    if not args.get('storage'):
        raise ValueError(
            "invalid configuration; missing 'storage' config entry.")

    storage = get_storage(**args.pop('storage'))
    backend = get_vault(**vcfg)

    return cooker_cls(obj_type, obj_id,
                      backend=backend, storage=storage,
                      max_bundle_size=cfg['max_bundle_size'])
