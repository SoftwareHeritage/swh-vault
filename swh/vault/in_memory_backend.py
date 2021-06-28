# Copyright (C) 2017-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, List, Optional, Tuple, Union

from swh.model.hashutil import hash_to_bytes

from .cache import VaultCache

ObjectId = Union[str, bytes]


class InMemoryVaultBackend:
    """Stub vault backend, for use in the CLI."""

    def __init__(self):
        self._cache = VaultCache(cls="memory")

    def fetch(self, obj_type: str, obj_id: ObjectId) -> Optional[bytes]:
        return self._cache.get(obj_type, hash_to_bytes(obj_id))

    def cook(
        self, obj_type: str, obj_id: ObjectId, email: Optional[str] = None
    ) -> Dict[str, Any]:
        raise NotImplementedError("InMemoryVaultBackend.cook()")

    def progress(self, obj_type: str, obj_id: ObjectId):
        raise NotImplementedError("InMemoryVaultBackend.progress()")

    # Cookers endpoints

    def set_progress(self, obj_type: str, obj_id: ObjectId, progress: str) -> None:
        pass

    def set_status(self, obj_type: str, obj_id: ObjectId, status: str) -> None:
        pass

    def put_bundle(self, obj_type: str, obj_id: ObjectId, bundle) -> bool:
        self._cache.add(obj_type, hash_to_bytes(obj_id), bundle)
        return True

    def send_notif(self, obj_type: str, obj_id: ObjectId):
        pass

    # Batch endpoints

    def batch_cook(self, batch: List[Tuple[str, str]]) -> int:
        raise NotImplementedError("InMemoryVaultBackend.batch_cook()")

    def batch_progress(self, batch_id: int) -> Dict[str, Any]:
        pass
