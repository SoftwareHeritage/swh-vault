# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU Affero General Public License version 3, or any later version
# See top-level LICENSE file for more information
import logging

logger = logging.getLogger(__name__)


def get_vault(cls='remote', args={}):
    """
    Get a vault object of class `vault_class` with arguments
    `vault_args`.

    Args:
        vault (dict): dictionary with keys:
        - cls (str): vault's class, either 'remote'
        - args (dict): dictionary with keys

    Returns:
        an instance of VaultBackend (either local or remote)

    Raises:
        ValueError if passed an unknown storage class.

    """
    if cls == 'remote':
        from .api.client import RemoteVaultClient as Vault
    elif cls == 'local':
        from swh.scheduler import get_scheduler
        from swh.storage import get_storage
        from swh.vault.cache import VaultCache
        from swh.vault.backend import VaultBackend as Vault
        args['cache'] = VaultCache(**args['cache'])
        args['storage'] = get_storage(**args['storage'])
        args['scheduler'] = get_scheduler(**args['scheduler'])
    else:
        raise ValueError('Unknown storage class `%s`' % cls)
    logger.debug('Instantiating %s with %s' % (Vault, args))
    return Vault(**args)
