# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model import hashutil
from swh.core.api import SWHRemoteAPI


class VaultAPIError(Exception):
    """Vault API Error"""
    def __str__(self):
        return ('An unexpected error occurred in the Vault backend: {}'
                .format(self.args))


class RemoteVaultClient(SWHRemoteAPI):
    """Client to the Software Heritage vault cache."""

    def __init__(self, base_url):
        super().__init__(api_exception=VaultAPIError, url=base_url)

    # Web API endpoints

    def fetch(self, obj_type, obj_id):
        hex_id = hashutil.hash_to_hex(obj_id)
        return self.get('fetch/{}/{}'.format(obj_type, hex_id))

    def cook(self, obj_type, obj_id, email=None):
        hex_id = hashutil.hash_to_hex(obj_id)
        return self.post('cook/{}/{}'.format(obj_type, hex_id),
                         data={},
                         params=({'email': email} if email else None))

    def progress(self, obj_type, obj_id):
        hex_id = hashutil.hash_to_hex(obj_id)
        return self.get('progress/{}/{}'.format(obj_type, hex_id))

    # Cookers endpoints

    def set_progress(self, obj_type, obj_id, progress):
        hex_id = hashutil.hash_to_hex(obj_id)
        return self.post('set_progress/{}/{}'.format(obj_type, hex_id),
                         data=progress)

    def set_status(self, obj_type, obj_id, status):
        hex_id = hashutil.hash_to_hex(obj_id)
        return self.post('set_status/{}/{}' .format(obj_type, hex_id),
                         data=status)

    # TODO: handle streaming properly
    def put_bundle(self, obj_type, obj_id, bundle):
        hex_id = hashutil.hash_to_hex(obj_id)
        return self.post('put_bundle/{}/{}' .format(obj_type, hex_id),
                         data=bundle)

    def send_notif(self, obj_type, obj_id):
        hex_id = hashutil.hash_to_hex(obj_id)
        return self.post('send_notif/{}/{}' .format(obj_type, hex_id),
                         data=None)
