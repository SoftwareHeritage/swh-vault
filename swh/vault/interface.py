# Copyright (C) 2017-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

from typing_extensions import Protocol, runtime_checkable

from swh.core.api import remote_api_endpoint
from swh.model.swhids import CoreSWHID


@runtime_checkable
class VaultInterface(Protocol):
    """
    Backend Interface for the Software Heritage vault.
    """

    @remote_api_endpoint("fetch")
    def fetch(self, bundle_type: str, swhid: CoreSWHID) -> Optional[bytes]:
        """Fetch information from a bundle"""
        ...

    @remote_api_endpoint("download_url")
    def download_url(
        self,
        bundle_type: str,
        swhid: CoreSWHID,
        content_disposition: Optional[str] = None,
        expiry: Optional[timedelta] = None,
    ) -> Optional[str]:
        """Obtain bundle direct download link if the vault cache backend supports it."""
        ...

    @remote_api_endpoint("cook")
    def cook(
        self, bundle_type: str, swhid: CoreSWHID, email: Optional[str] = None
    ) -> Dict[str, Any]:
        """Main entry point for cooking requests. This starts a cooking task if
        needed, and add the given e-mail to the notify list"""
        ...

    @remote_api_endpoint("progress")
    def progress(self, bundle_type: str, swhid: CoreSWHID): ...

    # Cookers endpoints

    @remote_api_endpoint("set_progress")
    def set_progress(self, bundle_type: str, swhid: CoreSWHID, progress: str) -> None:
        """Set the cooking progress of a bundle"""
        ...

    @remote_api_endpoint("set_status")
    def set_status(self, bundle_type: str, swhid: CoreSWHID, status: str) -> bool:
        """Set the cooking status of a bundle"""
        ...

    @remote_api_endpoint("put_bundle")
    def put_bundle(self, bundle_type: str, swhid: CoreSWHID, bundle):
        """Store bundle in vault cache"""
        ...

    @remote_api_endpoint("send_notif")
    def send_notif(self, bundle_type: str, swhid: CoreSWHID):
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
