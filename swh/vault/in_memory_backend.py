# Copyright (C) 2017-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

from swh.model.swhids import CoreSWHID

from .cache import VaultCache


class InMemoryVaultBackend:
    """Stub vault backend, for use in the CLI."""

    def __init__(self):
        self._cache = VaultCache(cls="memory")

    def fetch(self, bundle_type: str, swhid: CoreSWHID) -> Optional[bytes]:
        return self._cache.get(bundle_type, swhid)

    def download_url(
        self,
        bundle_type: str,
        swhid: CoreSWHID,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        return None

    def cook(
        self, bundle_type: str, swhid: CoreSWHID, email: Optional[str] = None
    ) -> Dict[str, Any]:
        raise NotImplementedError("InMemoryVaultBackend.cook()")

    def progress(self, bundle_type: str, swhid: CoreSWHID):
        raise NotImplementedError("InMemoryVaultBackend.progress()")

    # Cookers endpoints

    def set_progress(self, bundle_type: str, swhid: CoreSWHID, progress: str) -> None:
        pass

    def set_status(self, bundle_type: str, swhid: CoreSWHID, status: str) -> None:
        pass

    def put_bundle(self, bundle_type: str, swhid: CoreSWHID, bundle) -> bool:
        self._cache.add(bundle_type, swhid, bundle)
        return True

    def send_notif(self, bundle_type: str, swhid: CoreSWHID):
        pass

    # Batch endpoints

    def batch_cook(self, batch: List[Tuple[str, str]]) -> int:
        raise NotImplementedError("InMemoryVaultBackend.batch_cook()")

    def batch_progress(self, batch_id: int) -> Dict[str, Any]:
        return {}
