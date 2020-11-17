# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

import os
from typing import Any, Dict

from swh.core.config import load_named_config
from swh.core.config import read as read_config
from swh.storage import get_storage
from swh.vault import get_vault
from swh.vault.cookers.base import DEFAULT_CONFIG, DEFAULT_CONFIG_PATH
from swh.vault.cookers.directory import DirectoryCooker
from swh.vault.cookers.revision_flat import RevisionFlatCooker
from swh.vault.cookers.revision_gitfast import RevisionGitfastCooker

COOKER_TYPES = {
    "directory": DirectoryCooker,
    "revision_flat": RevisionFlatCooker,
    "revision_gitfast": RevisionGitfastCooker,
}


def get_cooker_cls(obj_type):
    return COOKER_TYPES[obj_type]


def check_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure the configuration is ok to run a vault worker, and propagate defaults

    Raises:
        EnvironmentError if the configuration is not for remote instance
        ValueError if one of the following keys is missing: vault, storage

    Returns:
        New configuration dict to instantiate a vault worker instance

    """
    cfg = cfg.copy()

    if "vault" not in cfg:
        raise ValueError("missing 'vault' configuration")

    vcfg = cfg["vault"]
    if vcfg["cls"] != "remote":
        raise EnvironmentError(
            "This vault backend can only be a 'remote' configuration"
        )

    # TODO: Soft-deprecation of args key. Remove when ready.
    vcfg.update(vcfg.get("args", {}))

    # Default to top-level value if any
    if "storage" not in vcfg:
        vcfg["storage"] = cfg.get("storage")

    if not vcfg.get("storage"):
        raise ValueError("invalid configuration: missing 'storage' config entry.")

    return cfg


def get_cooker(obj_type: str, obj_id: str):
    """Instantiate a cooker class of type obj_type.

    Returns:
        Cooker class in charge of cooking the obj_type with id obj_id.

    Raises:
        ValueError in case of a missing top-level vault key configuration or a storage
          key.
        EnvironmentError in case the vault configuration reference a non remote class.

    """
    if "SWH_CONFIG_FILENAME" in os.environ:
        cfg = read_config(os.environ["SWH_CONFIG_FILENAME"], DEFAULT_CONFIG)
    else:
        cfg = load_named_config(DEFAULT_CONFIG_PATH, DEFAULT_CONFIG)
    cooker_cls = get_cooker_cls(obj_type)

    cfg = check_config(cfg)
    vcfg = cfg["vault"]

    storage = get_storage(**vcfg.pop("storage"))
    backend = get_vault(**vcfg)

    return cooker_cls(
        obj_type,
        obj_id,
        backend=backend,
        storage=storage,
        max_bundle_size=cfg["max_bundle_size"],
    )
