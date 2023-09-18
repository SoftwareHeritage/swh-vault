# Copyright (C) 2016-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
from typing import Optional

from swh.model import hashutil
from swh.model.swhids import CoreSWHID
from swh.objstorage.factory import get_objstorage
from swh.objstorage.interface import ObjId


class VaultCache:
    """The Vault cache is an object storage that stores Vault bundles.

    This implementation computes sha1('<bundle_type>:<swhid>') as the
    internal identifiers used in the underlying objstorage.
    """

    def __init__(self, **objstorage):
        self.objstorage = get_objstorage(**objstorage)

    def add(self, bundle_type, swhid: CoreSWHID, content) -> None:
        sid = self._get_internal_id(bundle_type, swhid)
        self.objstorage.add(content, sid)

    def get(self, bundle_type, swhid: CoreSWHID) -> bytes:
        sid = self._get_internal_id(bundle_type, swhid)
        return self.objstorage.get(sid)

    def download_url(
        self,
        bundle_type,
        swhid: CoreSWHID,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        sid = self._get_internal_id(bundle_type, swhid)
        return self.objstorage.download_url(sid, content_disposition, expiry)

    def delete(self, bundle_type, swhid: CoreSWHID):
        sid = self._get_internal_id(bundle_type, swhid)
        return self.objstorage.delete(sid)

    def is_cached(self, bundle_type, swhid: CoreSWHID) -> bool:
        sid = self._get_internal_id(bundle_type, swhid)
        return sid in self.objstorage

    def _get_internal_id(self, bundle_type, swhid: CoreSWHID) -> ObjId:
        key = "{}:{}".format(bundle_type, swhid).encode()
        return hashutil.MultiHash.from_data(key).digest()
