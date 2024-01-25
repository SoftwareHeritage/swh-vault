# Copyright (C) 2017-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

import os
from typing import Any, Dict, List, Type

from swh.core.config import load_named_config
from swh.core.config import read as read_config
from swh.model.swhids import CoreSWHID, ObjectType
from swh.objstorage.factory import get_objstorage
from swh.storage import get_storage
from swh.vault.cookers.base import DEFAULT_CONFIG, DEFAULT_CONFIG_PATH, BaseVaultCooker
from swh.vault.cookers.directory import DirectoryCooker
from swh.vault.cookers.git_bare import GitBareCooker
from swh.vault.cookers.revision_flat import RevisionFlatCooker
from swh.vault.cookers.revision_gitfast import RevisionGitfastCooker

_COOKER_CLS: List[Type[BaseVaultCooker]] = [
    DirectoryCooker,
    RevisionFlatCooker,
    RevisionGitfastCooker,
    GitBareCooker,
]
COOKER_TYPES: Dict[str, List[Type[BaseVaultCooker]]] = {}


for _cooker_cls in _COOKER_CLS:
    COOKER_TYPES.setdefault(_cooker_cls.BUNDLE_TYPE, []).append(_cooker_cls)


def get_cooker_cls(bundle_type: str, object_type: ObjectType):
    cookers = COOKER_TYPES.get(bundle_type)

    if not cookers:
        raise ValueError(f"{bundle_type} is not a valid bundle type.")

    for cooker in cookers:
        try:
            cooker.check_object_type(object_type)
        except ValueError:
            pass
        else:
            return cooker

    raise ValueError(
        f"{object_type.name.lower()} objects do not have a {bundle_type} cooker"
    )


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

    # Default to top-level value if any
    for key in ("storage", "objstorage", "graph"):
        if key not in vcfg and key in cfg:
            vcfg[key] = cfg[key]

    if not vcfg.get("storage"):
        raise ValueError("invalid configuration: missing 'storage' config entry.")

    return cfg


def get_cooker(bundle_type: str, swhid: CoreSWHID):
    """Instantiate a cooker class of type bundle_type.

    Returns:
        Cooker class in charge of cooking the bundle_type with id swhid.

    Raises:
        ValueError in case of a missing top-level vault key configuration or a storage
          key.
        EnvironmentError in case the vault configuration reference a non remote class.

    """
    from swh.vault import get_vault

    if "SWH_CONFIG_FILENAME" in os.environ:
        cfg = read_config(os.environ["SWH_CONFIG_FILENAME"], DEFAULT_CONFIG)
    else:
        cfg = load_named_config(DEFAULT_CONFIG_PATH, DEFAULT_CONFIG)
    cooker_cls = get_cooker_cls(bundle_type, swhid.object_type)

    cfg = check_config(cfg)
    vcfg = cfg["vault"]

    storage = get_storage(**vcfg.pop("storage"))
    backend = get_vault(**vcfg)

    try:
        from swh.graph.http_client import RemoteGraphClient  # optional dependency

        graph = RemoteGraphClient(**vcfg["graph"]) if vcfg.get("graph") else None
    except ModuleNotFoundError:
        if vcfg.get("graph"):
            raise EnvironmentError(
                "Graph configuration required but module is not installed."
            )
        else:
            graph = None

    if vcfg.get("objstorage"):
        objstorage = get_objstorage(**vcfg["objstorage"])
    else:
        objstorage = None

    kwargs = {
        k: v for (k, v) in cfg.items() if k in ("max_bundle_size", "thread_pool_size")
    }

    return cooker_cls(
        swhid,
        backend=backend,
        storage=storage,
        objstorage=objstorage,
        graph=graph,
        **kwargs,
    )
