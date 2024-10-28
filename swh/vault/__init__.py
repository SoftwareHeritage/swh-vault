# Copyright (C) 2018-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Dict

if TYPE_CHECKING:
    from .interface import VaultInterface

logger = logging.getLogger(__name__)


def get_vault(cls: str, **kwargs) -> "VaultInterface":
    """
    Get a vault object of class `vault_class` with arguments
    `vault_args`.

    Args:
        cls: vault's class
        kwargs: arguments to pass to the class' constructor

    Returns:
        an instance of VaultBackend

    Raises:
        ValueError if passed an unknown storage class.

    """
    from swh.core.config import get_swh_backend_module

    _, Vault = get_swh_backend_module("vault", cls)
    assert Vault is not None
    return Vault(**kwargs)


default_cfg = {
    "default_interval": "1 day",
    "min_interval": "1 day",
    "max_interval": "1 day",
    "backoff_factor": 1,
    "max_queue_length": 10000,
}


def register_tasks() -> Dict[str, Any]:
    return {
        "task_modules": [f"{__name__}.cooking_tasks"],
        "task_types": {
            "vault-cook-bundle": default_cfg,
            "vault-batch-cook-bundle": default_cfg,
        },
    }
