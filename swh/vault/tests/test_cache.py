# Copyright (C) 2017-2022  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.model.swhids import CoreSWHID

TEST_TYPE_1 = "revision_gitfast"
TEST_TYPE_2 = "directory"

TEST_SWHID_1 = CoreSWHID.from_string(
    "swh:1:rev:4a4b9771542143cf070386f86b4b92d42966bdbc"
)
TEST_SWHID_2 = CoreSWHID.from_string(
    "swh:1:dir:17a3e48bce37be5226490e750202ad3a9a1a3fe9"
)

TEST_CONTENT_1 = b"test content 1"
TEST_CONTENT_2 = b"test content 2"


# Let's try to avoid replicating edge-cases already tested in
# swh-objstorage, and instead focus on testing behaviors specific to the
# Vault cache here.


def test_internal_id(swh_vault):
    sid = swh_vault.cache._get_internal_id(TEST_TYPE_1, TEST_SWHID_1)
    assert sid["sha1"].hex() == "ec2a99d6b21a68648a9d0c99c5d7c35f69268564"


def test_simple_add_get(swh_vault):
    swh_vault.cache.add(TEST_TYPE_1, TEST_SWHID_1, TEST_CONTENT_1)
    assert swh_vault.cache.get(TEST_TYPE_1, TEST_SWHID_1) == TEST_CONTENT_1
    assert swh_vault.cache.is_cached(TEST_TYPE_1, TEST_SWHID_1)


def test_different_type_same_id(swh_vault):
    swh_vault.cache.add(TEST_TYPE_1, TEST_SWHID_1, TEST_CONTENT_1)
    swh_vault.cache.add(TEST_TYPE_2, TEST_SWHID_1, TEST_CONTENT_2)
    assert swh_vault.cache.get(TEST_TYPE_1, TEST_SWHID_1) == TEST_CONTENT_1
    assert swh_vault.cache.get(TEST_TYPE_2, TEST_SWHID_1) == TEST_CONTENT_2
    assert swh_vault.cache.is_cached(TEST_TYPE_1, TEST_SWHID_1)
    assert swh_vault.cache.is_cached(TEST_TYPE_2, TEST_SWHID_1)


def test_different_type_same_content(swh_vault):
    swh_vault.cache.add(TEST_TYPE_1, TEST_SWHID_1, TEST_CONTENT_1)
    swh_vault.cache.add(TEST_TYPE_2, TEST_SWHID_1, TEST_CONTENT_1)
    assert swh_vault.cache.get(TEST_TYPE_1, TEST_SWHID_1) == TEST_CONTENT_1
    assert swh_vault.cache.get(TEST_TYPE_2, TEST_SWHID_1) == TEST_CONTENT_1
    assert swh_vault.cache.is_cached(TEST_TYPE_1, TEST_SWHID_1)
    assert swh_vault.cache.is_cached(TEST_TYPE_2, TEST_SWHID_1)


def test_different_id_same_type(swh_vault):
    swh_vault.cache.add(TEST_TYPE_1, TEST_SWHID_1, TEST_CONTENT_1)
    swh_vault.cache.add(TEST_TYPE_1, TEST_SWHID_2, TEST_CONTENT_2)
    assert swh_vault.cache.get(TEST_TYPE_1, TEST_SWHID_1) == TEST_CONTENT_1
    assert swh_vault.cache.get(TEST_TYPE_1, TEST_SWHID_2) == TEST_CONTENT_2
    assert swh_vault.cache.is_cached(TEST_TYPE_1, TEST_SWHID_1)
    assert swh_vault.cache.is_cached(TEST_TYPE_1, TEST_SWHID_2)
