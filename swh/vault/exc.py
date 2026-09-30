# Copyright (C) 2020-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


class NotFoundExc(Exception):
    pass


class PolicyError(Exception):
    """Raised when the bundle violates the cooking policy."""

    pass


class BundleTooLargeError(PolicyError):
    """Raised when the bundle is too large to be cooked."""

    pass


class DirectoryTooLargeError(PolicyError):
    """Raised when a directory tree expands to more than the vault materializes.

    ``exceeded`` names what was exceeded, eg. "more than 100000 files and
    directories".
    """

    def __init__(self, swhid, exceeded: str):
        super().__init__(
            f"{swhid} expands to {exceeded}, which is more than this bundle "
            f"type writes out. Cooking one of its sub-directories, or cooking "
            f"it as a 'git_bare' bundle instead, may work."
        )
