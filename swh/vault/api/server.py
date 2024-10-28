# Copyright (C) 2016-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from swh.core.api import RPCServerApp
from swh.core.api import encode_data_server as encode_data
from swh.core.api import error_handler
from swh.core.config import merge_configs, read_raw_config
from swh.vault import get_vault as get_swhvault
from swh.vault.backend import NotFoundExc
from swh.vault.interface import VaultInterface

from .serializers import DECODERS, ENCODERS

# do not define default services here
DEFAULT_CONFIG = {
    "client_max_size": 1024**3,
}


def get_vault():
    global vault
    if not vault:
        vault = get_swhvault(**app.config["vault"])

    return vault


class VaultServerApp(RPCServerApp):
    extra_type_decoders = DECODERS
    extra_type_encoders = ENCODERS


vault = None
app = VaultServerApp(
    __name__,
    backend_class=VaultInterface,
    backend_factory=get_vault,
)


@app.errorhandler(NotFoundExc)
def argument_error_handler(exception):
    return error_handler(exception, encode_data, status_code=400)


@app.errorhandler(Exception)
def my_error_handler(exception):
    return error_handler(exception, encode_data)


@app.route("/")
def index():
    return "SWH Vault API server"


def check_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure the configuration is ok to run a postgresql vault server, and propagate
    defaults.

    Raises:
        EnvironmentError if the configuration is not for postgresql instance
        ValueError if one of the following keys is missing: vault, cache, storage,
        scheduler

    Returns:
        New configuration dict to instantiate a postgresql vault server instance.

    """
    cfg = cfg.copy()

    if "vault" not in cfg:
        raise ValueError("missing 'vault' configuration")

    vcfg = cfg["vault"]
    if vcfg["cls"] == "remote":
        raise EnvironmentError(
            "The vault backend of a vault server cannot be a 'remote' configuration"
        )

    # Default to top-level value if any
    vcfg = {**cfg, **vcfg}

    for key in ("cache", "storage", "scheduler"):
        if not vcfg.get(key):
            raise ValueError(f"invalid configuration: missing {key} config entry.")

    return vcfg


def make_app_from_configfile(
    config_path: Optional[str] = None, **kwargs
) -> VaultServerApp:
    """Load and check configuration if ok, then instantiate (once) a vault server
    application.

    """
    config_path = os.environ.get("SWH_CONFIG_FILENAME", config_path)
    if not config_path:
        raise ValueError("Missing configuration path.")
    if not os.path.isfile(config_path):
        raise ValueError(f"Configuration path {config_path} should exist.")

    app_config = read_raw_config(config_path)
    app_config["vault"] = check_config(app_config)
    app.config.update(merge_configs(DEFAULT_CONFIG, app_config))

    return app


if __name__ == "__main__":
    print("Deprecated. Use swh-vault ")
