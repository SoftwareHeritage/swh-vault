# Copyright (C) 2017-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict, List, Optional, Tuple, Union

from typing_extensions import Protocol, runtime_checkable

from swh.core.api import remote_api_endpoint

ObjectId = Union[str, bytes]


@runtime_checkable
class VaultInterface(Protocol):
    """
    Backend Interface for the Software Heritage vault.
    """

    @remote_api_endpoint("fetch")
    def fetch(self, obj_type: str, obj_id: ObjectId) -> Dict[str, Any]:
        """Fetch information from a bundle"""
        ...

    @remote_api_endpoint("cook")
    def cook(
        self, obj_type: str, obj_id: ObjectId, email: Optional[str] = None
    ) -> Dict[str, Any]:
        """Main entry point for cooking requests. This starts a cooking task if
            needed, and add the given e-mail to the notify list"""
        ...

    @remote_api_endpoint("progress")
    def progress(self, obj_type: str, obj_id: ObjectId):
        ...

    # Cookers endpoints

    @remote_api_endpoint("set_progress")
    def set_progress(self, obj_type: str, obj_id: ObjectId, progress: str) -> None:
        """Set the cooking progress of a bundle"""
        ...

    @remote_api_endpoint("set_status")
    def set_status(self, obj_type: str, obj_id: ObjectId, status: str) -> None:
        """Set the cooking status of a bundle"""
        ...

    @remote_api_endpoint("put_bundle")
    def put_bundle(self, obj_type: str, obj_id: ObjectId, bundle):
        """Store bundle in vault cache"""
        ...

    @remote_api_endpoint("send_notif")
    def send_notif(self, obj_type: str, obj_id: ObjectId):
        """Send all the e-mails in the notification list of a bundle"""
        ...

    # Batch endpoints

    @remote_api_endpoint("batch_cook")
    def batch_cook(self, batch: List[Tuple[str, str]]) -> int:
        """Cook a batch of bundles and returns the cooking id."""
        ...

    @remote_api_endpoint("batch_progress")
    def batch_progress(self, batch_id: int) -> Dict[str, Any]:
        """Fetch information from a batch of bundles"""
        ...
