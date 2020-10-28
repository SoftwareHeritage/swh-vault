# Copyright (C) 2016-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, Optional

import aiohttp.web

from swh.core.api.asynchronous import RPCServerApp
from swh.core.config import config_basepath, merge_configs, read_raw_config
from swh.vault import get_vault as get_swhvault
from swh.vault.backend import NotFoundExc
from swh.vault.interface import VaultInterface

DEFAULT_CONFIG = {
    "storage": {"cls": "remote", "url": "http://localhost:5002/"},
    "cache": {
        "cls": "pathslicing",
        "args": {"root": "/srv/softwareheritage/vault", "slicing": "0:1/1:5"},
    },
    "client_max_size": 1024 ** 3,
    "vault": {"cls": "local", "args": {"db": "dbname=softwareheritage-vault-dev",}},
    "scheduler": {"cls": "remote", "url": "http://localhost:5008/"},
}


vault = None
app = None


def get_vault(config: Optional[Dict[str, Any]] = None) -> VaultInterface:
    global vault
    if not vault:
        assert config is not None
        vault = get_swhvault(**config)
    return vault


class VaultServerApp(RPCServerApp):
    client_exception_classes = (NotFoundExc,)


@asyncio.coroutine
def index(request):
    return aiohttp.web.Response(body="SWH Vault API server")


def check_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure the configuration is ok to run a local vault server, and propagate defaults.

    Raises:
        EnvironmentError if the configuration is not for local instance
        ValueError if one of the following keys is missing: vault, cache, storage,
        scheduler

    Returns:
        New configuration dict to instantiate a local vault server instance.

    """
    cfg = cfg.copy()

    if "vault" not in cfg:
        raise ValueError("missing 'vault' configuration")

    vcfg = cfg["vault"]
    if vcfg["cls"] != "local":
        raise EnvironmentError(
            "The vault backend can only be started with a 'local' configuration",
        )

    # TODO: Soft-deprecation of args key. Remove when ready.
    vcfg.update(vcfg.get("args", {}))

    # Default to top-level value if any
    if "cache" not in vcfg:
        vcfg["cache"] = cfg.get("cache")
    if "storage" not in vcfg:
        vcfg["storage"] = cfg.get("storage")
    if "scheduler" not in vcfg:
        vcfg["scheduler"] = cfg.get("scheduler")

    for key in ("cache", "storage", "scheduler"):
        if not vcfg.get(key):
            raise ValueError(f"invalid configuration: missing {key} config entry.")

    return cfg


def make_app(config: Dict[str, Any]) -> VaultServerApp:
    """Ensure the configuration is ok, then instantiate the server application

    """
    config = check_config(config)
    app = VaultServerApp(
        __name__,
        backend_class=VaultInterface,
        backend_factory=lambda: get_vault(config["vault"]),
        client_max_size=config["client_max_size"],
    )
    app.router.add_route("GET", "/", index)
    return app


def make_app_from_configfile(
    config_path: Optional[str] = None, **kwargs
) -> VaultServerApp:
    """Load and check configuration if ok, then instantiate (once) a vault server
       application.

    """
    global app
    if not app:
        config_path = os.environ.get("SWH_CONFIG_FILENAME", config_path)
        if not config_path:
            raise ValueError("Missing configuration path.")
        if not os.path.isfile(config_path):
            raise ValueError(f"Configuration path {config_path} should exist.")

        app_config = read_raw_config(config_basepath(config_path))
        app_config = merge_configs(DEFAULT_CONFIG, app_config)
        app = make_app(app_config)

    return app


if __name__ == "__main__":
    print("Deprecated. Use swh-vault ")
