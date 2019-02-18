# Copyright (C) 2016-2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model import hashutil
from swh.objstorage import get_objstorage
from swh.objstorage.objstorage import compute_hash


class VaultCache:
    """The Vault cache is an object storage that stores Vault bundles.

    This implementation computes sha1('<bundle_type>:<object_id>') as the
    internal identifiers used in the underlying objstorage.
    """

    def __init__(self, **objstorage):
        self.objstorage = get_objstorage(**objstorage)

    def add(self, obj_type, obj_id, content):
        sid = self._get_internal_id(obj_type, obj_id)
        return self.objstorage.add(content, sid)

    def get(self, obj_type, obj_id):
        sid = self._get_internal_id(obj_type, obj_id)
        return self.objstorage.get(hashutil.hash_to_bytes(sid))

    def delete(self, obj_type, obj_id):
        sid = self._get_internal_id(obj_type, obj_id)
        return self.objstorage.delete(hashutil.hash_to_bytes(sid))

    def add_stream(self, obj_type, obj_id, content_iter):
        sid = self._get_internal_id(obj_type, obj_id)
        return self.objstorage.add_stream(content_iter, sid)

    def get_stream(self, obj_type, obj_id):
        sid = self._get_internal_id(obj_type, obj_id)
        return self.objstorage.get_stream(hashutil.hash_to_bytes(sid))

    def is_cached(self, obj_type, obj_id):
        sid = self._get_internal_id(obj_type, obj_id)
        return hashutil.hash_to_bytes(sid) in self.objstorage

    def _get_internal_id(self, obj_type, obj_id):
        obj_id = hashutil.hash_to_hex(obj_id)
        return compute_hash('{}:{}'.format(obj_type, obj_id).encode())
