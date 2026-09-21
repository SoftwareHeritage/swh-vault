# Copyright (C) 2016-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import collections
import concurrent
import logging
import os
import sys
import time
from time import monotonic
from typing import Any, Dict, NoReturn, Optional, Tuple

from swh.model import hashutil
from swh.model.from_disk import DentryPerms, mode_to_perms
from swh.objstorage.interface import ObjStorageInterface, objid_from_dict
from swh.storage.interface import StorageInterface
from swh.vault.exc import DirectoryTooLargeError

MISSING_MESSAGE = (
    b"This content is missing from the Software Heritage archive "
    b"(or from the mirror used while retrieving it)."
)

SKIPPED_MESSAGE = (
    b"This content has not been retrieved in the "
    b"Software Heritage archive due to its size."
)

HIDDEN_MESSAGE = b"This content is hidden."


logger = logging.getLogger(__name__)

if sys.version_info >= (3, 13):

    class ContentFetchesFailed(ExceptionGroup):
        pass

else:

    class ContentFetchesFailed(Exception):
        pass


def wait_for_contents(futures: set[concurrent.futures.Future], timeout: float = 10):
    """Wait for a set of Futures, re-raising exceptions as they happen."""
    logger.debug("Waiting for %d futures", len(futures))
    start = time.monotonic()

    while futures:
        done, futures = concurrent.futures.wait(
            futures, timeout=timeout, return_when=concurrent.futures.FIRST_EXCEPTION
        )

        exceptions = []
        for future in done:
            if exc := future.exception():
                exceptions.append(exc)

        if exceptions:
            for future in futures:
                future.cancel()

            if len(exceptions) == 1:
                raise exceptions[0]
            else:
                raise ContentFetchesFailed("Errors while fetching contents", exceptions)

        if futures:
            logger.info(
                "After %2.f seconds: %d futures pending",
                time.monotonic() - start,
                len(futures),
            )


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


# How many refusals are remembered. Only directories that already exceeded a
# limit go in here, which is a rare population by construction, so this is a
# few tens of kilobytes.
MAX_REFUSALS_REMEMBERED = 1024


class _RefusedDirectories:
    """Directories already found to expand past a limit.

    Only refusals are worth remembering. A build that succeeds counts as it
    writes, so its count costs nothing and there is nothing to reuse. A refusal
    is paid in full every time the same directory is asked for again:
    :meth:`swh.vault.backend.VaultBackend.cook` deletes a failed bundle and
    re-creates the task, so a repeated request really does walk again and
    re-create up to ``max_directory_entries`` entries on disk before aborting.

    An entry records "exceeds this budget", which cannot stop being true of an
    immutable object, so it never needs invalidating. It may be reused only for
    a budget at or below the one it was recorded at: exceeding 100 entries says
    nothing about exceeding a million. Elapsed time is deliberately absent: it
    depends on the load of the day, not on the object.

    Bounded, per process and unsynchronised: a lost race costs one extra walk
    and never a wrong answer.
    """

    def __init__(self, maxsize: int = MAX_REFUSALS_REMEMBERED):
        self._maxsize = maxsize
        self._over: Dict[bytes, Tuple[int, int]] = {}

    def refuses(
        self,
        dir_id: bytes,
        max_directory_entries: Optional[int],
        max_directory_size: Optional[int],
    ) -> bool:
        """Whether ``dir_id`` is already known to exceed this budget.

        A directory refused under one budget is refused under any budget at or
        below it in *both* dimensions: we do not record which of the two it
        exceeded, so a looser limit anywhere means walking again. ``None`` is an
        unbounded limit, which is below nothing.
        """
        known = self._over.get(dir_id)
        if known is None or max_directory_entries is None or max_directory_size is None:
            return False
        entries, size = known
        return max_directory_entries <= entries and max_directory_size <= size

    def record(
        self,
        dir_id: bytes,
        max_directory_entries: Optional[int],
        max_directory_size: Optional[int],
    ) -> None:
        if len(self._over) >= self._maxsize:
            self._over.clear()
        if max_directory_entries is None or max_directory_size is None:
            return  # nothing reusable: an unbounded budget is below nothing
        self._over[dir_id] = (max_directory_entries, max_directory_size)

    def clear(self) -> None:
        self._over.clear()


# Process-wide: the point is to reuse a refusal *between* requests.
REFUSED_DIRECTORIES = _RefusedDirectories()


class DirectoryBuilder:
    """Reconstructs the on-disk representation of a directory in the storage."""

    def __init__(
        self,
        storage: StorageInterface,
        root: bytes,
        dir_id: bytes,
        thread_pool_size: int = 10,
        objstorage: Optional[ObjStorageInterface] = None,
        max_directory_entries: Optional[int] = None,
        max_directory_size: Optional[int] = None,
        max_cooking_time: Optional[int] = None,
    ):
        """Initialize the directory builder.

        Args:
            storage: the storage object
            root: the path where the directory should be reconstructed
            dir_id: the identifier of the directory in the storage
            max_directory_entries: refuse to write out more than this many entries,
                counted once per path; ``None`` disables the limit
            max_directory_size: refuse to write out more than this many bytes,
                counted once per path; ``None`` disables the limit
            max_cooking_time: give up after this many seconds of walking; ``None``
                disables the limit
        """
        self.storage = storage
        self.root = root
        self.dir_id = dir_id
        self.thread_pool_size = thread_pool_size
        self.objstorage = objstorage
        self.max_directory_entries = max_directory_entries
        self.max_directory_size = max_directory_size
        self.max_cooking_time = max_cooking_time

    def build(self) -> None:
        """Perform the reconstruction of the directory in the given root."""
        if REFUSED_DIRECTORIES.refuses(
            self.dir_id, self.max_directory_entries, self.max_directory_size
        ):
            self._refuse("more than one of the configured limits")

        def file_fetcher(file_data: Dict[str, Any]) -> None:
            file_data = get_filtered_file_content(
                self.storage, file_data, self.objstorage
            )
            path = os.path.join(self.root, file_data["path"])
            self._create_file(path, file_data["content"], file_data["perms"])

        with concurrent.futures.ThreadPoolExecutor(self.thread_pool_size) as executor:
            futures = set()

            os.makedirs(self.root, exist_ok=True)
            queue = collections.deque([(b"", self.dir_id)])
            entries = 0
            size = 0
            started = monotonic()
            while queue:
                path, dir_id = queue.popleft()
                dir_entries = self.storage.directory_ls(dir_id)

                for dir_entry in dir_entries:
                    entries += 1
                    # `length` is already in the entry, so the byte budget
                    # costs no extra call; it is None for anything but a file.
                    size += dir_entry["length"] or 0
                    self._check_budget(entries, size, monotonic() - started)
                    dir_entry["path"] = os.path.join(path, dir_entry["name"])
                    match dir_entry["type"]:
                        case "dir":
                            self._create_tree(dir_entry)
                            queue.append((dir_entry["path"], dir_entry["target"]))
                        case "rev":
                            self._create_revision(dir_entry)
                        case "file":
                            futures.add(executor.submit(file_fetcher, dir_entry))
                        case _:
                            raise ValueError(
                                f"Unsupported directory entry type "
                                f"{dir_entry['type']} for {dir_entry['name']:r} in "
                                f"directory swh:1:dir:{dir_id.hex()}"
                            )

            wait_for_contents(futures)

    def _check_budget(self, entries: int, size: int, elapsed: float) -> None:
        """Refuse as soon as any of the three budgets is passed.
        """
        if self.max_cooking_time is not None and elapsed > self.max_cooking_time:
            # not remembered: how long a walk took is a property of the day,
            # not of an immutable tree
            self._refuse(f"more than {self.max_cooking_time} seconds of walking")

        if (
            self.max_directory_entries is not None
            and entries > self.max_directory_entries
        ):
            exceeded = f"more than {self.max_directory_entries} files and directories"
        elif self.max_directory_size is not None and size > self.max_directory_size:
            exceeded = f"more than {self.max_directory_size} bytes"
        else:
            return

        REFUSED_DIRECTORIES.record(
            self.dir_id, self.max_directory_entries, self.max_directory_size
        )
        self._refuse(exceeded)

    def _refuse(self, exceeded: str) -> NoReturn:
        raise DirectoryTooLargeError(f"swh:1:dir:{self.dir_id.hex()}", exceeded)

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
