# Copyright (C) 2016-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model import hashutil
from swh.core.api import SWHRemoteAPI


class RemoteVaultClient(SWHRemoteAPI):
    """Client to the Software Heritage vault cache."""

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

    # Batch endpoints

    def batch_cook(self, batch):
        return self.post('batch_cook', data=batch)

    def batch_progress(self, batch_id):
        return self.get('batch_progress/{}'.format(batch_id))
