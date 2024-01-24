# Copyright (C) 2016-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import concurrent
import os
from typing import Any, Dict, Optional

from swh.model import hashutil
from swh.model.from_disk import DentryPerms, mode_to_perms
from swh.objstorage.interface import ObjStorageInterface, objid_from_dict
from swh.storage.interface import StorageInterface

MISSING_MESSAGE = (
    b"This content is missing from the Software Heritage archive "
    b"(or from the mirror used while retrieving it)."
)

SKIPPED_MESSAGE = (
    b"This content has not been retrieved in the "
    b"Software Heritage archive due to its size."
)

HIDDEN_MESSAGE = b"This content is hidden."


def get_filtered_file_content(
    storage: StorageInterface,
    file_data: Dict[str, Any],
    objstorage: Optional[ObjStorageInterface] = None,
) -> Dict[str, Any]:
    """Retrieve the file specified by file_data and apply filters for skipped
    and missing content.

    Args:
        storage: the storage from which to retrieve the objects
        file_data: a file entry as returned by directory_ls()

    Returns:
        The entry given in file_data with a new 'content' key that points to
        the file content in bytes.

        The contents can be replaced by a specific message to indicate that
        they could not be retrieved (either due to privacy policy or because
        their sizes were too big for us to archive it).

    """
    status = file_data["status"]
    if status == "visible":
        hashes = objid_from_dict(file_data)
        data: Optional[bytes]
        if objstorage is not None:
            data = objstorage.get(hashes)
        else:
            data = storage.content_get_data(hashes)
        if data is None:
            content = SKIPPED_MESSAGE
        else:
            content = data
    elif status == "absent":
        content = SKIPPED_MESSAGE
    elif status == "hidden":
        content = HIDDEN_MESSAGE
    elif status is None:
        content = MISSING_MESSAGE
    else:
        assert False, (
            f"unexpected status {status!r} "
            f"for content {hashutil.hash_to_hex(file_data['target'])}"
        )

    return {"content": content, **file_data}


class DirectoryBuilder:
    """Reconstructs the on-disk representation of a directory in the storage."""

    def __init__(
        self,
        storage: StorageInterface,
        root: bytes,
        dir_id: bytes,
        thread_pool_size: int = 10,
        objstorage: Optional[ObjStorageInterface] = None,
    ):
        """Initialize the directory builder.

        Args:
            storage: the storage object
            root: the path where the directory should be reconstructed
            dir_id: the identifier of the directory in the storage
        """
        self.storage = storage
        self.root = root
        self.dir_id = dir_id
        self.thread_pool_size = thread_pool_size
        self.objstorage = objstorage

    def build(self) -> None:
        """Perform the reconstruction of the directory in the given root."""

        def file_fetcher(file_data: Dict[str, Any]) -> None:
            file_data = get_filtered_file_content(
                self.storage, file_data, self.objstorage
            )
            path = os.path.join(self.root, file_data["path"])
            self._create_file(path, file_data["content"], file_data["perms"])

        executor = concurrent.futures.ThreadPoolExecutor(self.thread_pool_size)
        futures = []

        os.makedirs(self.root, exist_ok=True)
        queue = collections.deque([(b"", self.dir_id)])
        while queue:
            path, dir_id = queue.popleft()
            dir_entries = self.storage.directory_ls(dir_id)
            for dir_entry in dir_entries:
                dir_entry["path"] = os.path.join(path, dir_entry["name"])
                match dir_entry["type"]:
                    case "dir":
                        self._create_tree(dir_entry)
                        queue.append((dir_entry["path"], dir_entry["target"]))
                    case "rev":
                        self._create_revision(dir_entry)
                    case "file":
                        futures.append(executor.submit(file_fetcher, dir_entry))
                    case _:
                        raise ValueError(
                            f"Unsupported directory entry type {dir_entry['type']} for "
                            f"{dir_entry['name']:r} in directory swh:1:dir:{dir_id.hex()}"
                        )

        concurrent.futures.wait(futures)

    def _create_tree(self, directory: Dict[str, Any]) -> None:
        """Create a directory tree from root for the given path."""
        os.makedirs(os.path.join(self.root, directory["path"]), exist_ok=True)

    def _create_revision(self, rev_data: Dict[str, Any]) -> None:
        """Create the revision in the tree as a broken symlink to the target
        identifier."""
        os.makedirs(os.path.join(self.root, rev_data["path"]), exist_ok=True)

    def _create_file(
        self, path: bytes, content: bytes, mode: int = DentryPerms.content
    ) -> None:
        """Create the given file and fill it with content."""
        perms = mode_to_perms(mode)
        if perms == DentryPerms.symlink:
            os.symlink(content, path)
        else:
            with open(path, "wb") as f:
                f.write(content)
            os.chmod(path, perms.value)
