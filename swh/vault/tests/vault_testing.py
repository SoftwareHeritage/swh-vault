# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.model import hashutil


def hash_content(content):
    """Hash the content's id (sha1).

    Args:
        content (bytes): Content to hash

    Returns:
        The tuple (content, content's id as bytes)

    """
    hashes = hashutil.MultiHash.from_data(
        content, hash_names=['sha1']).digest()
    return content, hashes['sha1']
