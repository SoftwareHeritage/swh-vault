# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model import hashutil
from swh.core.api import SWHRemoteAPI
from swh.storage.exc import StorageAPIError


class RemoteVaultClient(SWHRemoteAPI):
    """Client to the Software Heritage vault cache."""

    def __init__(self, base_url):
        super().__init__(api_exception=StorageAPIError, url=base_url)

    def fetch(self, obj_type, obj_id):
        return self.get('fetch/{}/{}'.format(obj_type,
                                             hashutil.hash_to_hex(obj_id)))

    def cook(self, obj_type, obj_id, email=None):
        return self.post('cook/{}/{}'.format(obj_type,
                                             hashutil.hash_to_hex(obj_id)),
                         data={},
                         params=({'email': email} if email else None))

    def progress(self, obj_type, obj_id):
        return self.get('progress/{}/{}'.format(obj_type,
                                                hashutil.hash_to_hex(obj_id)))
