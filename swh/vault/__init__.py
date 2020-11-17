# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

import importlib
import logging
from typing import Dict
import warnings

logger = logging.getLogger(__name__)


BACKEND_TYPES: Dict[str, str] = {
    "remote": ".api.client.RemoteVaultClient",
    "local": ".backend.VaultBackend",
}


def get_vault(cls: str = "remote", **kwargs):
    """
    Get a vault object of class `vault_class` with arguments
    `vault_args`.

    Args:
        cls: vault's class, either 'remote' or 'local'
        kwargs: arguments to pass to the class' constructor

    Returns:
        an instance of VaultBackend (either local or remote)

    Raises:
        ValueError if passed an unknown storage class.

    """
    if "args" in kwargs:
        warnings.warn(
            'Explicit "args" key is deprecated, use keys directly instead.',
            DeprecationWarning,
        )
        kwargs = kwargs["args"]

    class_path = BACKEND_TYPES.get(cls)
    if class_path is None:
        raise ValueError(
            f"Unknown Vault class `{cls}`. " f"Supported: {', '.join(BACKEND_TYPES)}"
        )

    (module_path, class_name) = class_path.rsplit(".", 1)
    module = importlib.import_module(module_path, package=__package__)
    Vault = getattr(module, class_name)
    return Vault(**kwargs)
