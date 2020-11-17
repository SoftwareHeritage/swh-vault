# Copyright (C) 2016-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.core.api import RPCClient
from swh.vault.exc import NotFoundExc
from swh.vault.interface import VaultInterface


class RemoteVaultClient(RPCClient):
    """Client to the Software Heritage vault cache."""

    backend_class = VaultInterface
    reraise_exceptions = [NotFoundExc]
