# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.model import hashutil

TEST_TYPE_1 = 'revision_gitfast'
TEST_TYPE_2 = 'directory'

TEST_HEX_ID_1 = '4a4b9771542143cf070386f86b4b92d42966bdbc'
TEST_HEX_ID_2 = '17a3e48bce37be5226490e750202ad3a9a1a3fe9'

TEST_OBJ_ID_1 = hashutil.hash_to_bytes(TEST_HEX_ID_1)
TEST_OBJ_ID_2 = hashutil.hash_to_bytes(TEST_HEX_ID_2)

TEST_CONTENT_1 = b'test content 1'
TEST_CONTENT_2 = b'test content 2'


# Let's try to avoid replicating edge-cases already tested in
# swh-objstorage, and instead focus on testing behaviors specific to the
# Vault cache here.

def test_internal_id(swh_vault):
    sid = swh_vault.cache._get_internal_id(TEST_TYPE_1, TEST_OBJ_ID_1)
    assert hashutil.hash_to_hex(sid) == \
        '6829cda55b54c295aa043a611a4e0320239988d9'


def test_simple_add_get(swh_vault):
    swh_vault.cache.add(TEST_TYPE_1, TEST_OBJ_ID_1, TEST_CONTENT_1)
    assert swh_vault.cache.get(TEST_TYPE_1, TEST_OBJ_ID_1) == \
        TEST_CONTENT_1
    assert swh_vault.cache.is_cached(TEST_TYPE_1, TEST_OBJ_ID_1)


def test_different_type_same_id(swh_vault):
    swh_vault.cache.add(TEST_TYPE_1, TEST_OBJ_ID_1, TEST_CONTENT_1)
    swh_vault.cache.add(TEST_TYPE_2, TEST_OBJ_ID_1, TEST_CONTENT_2)
    assert swh_vault.cache.get(TEST_TYPE_1, TEST_OBJ_ID_1) == \
        TEST_CONTENT_1
    assert swh_vault.cache.get(TEST_TYPE_2, TEST_OBJ_ID_1) == \
        TEST_CONTENT_2
    assert swh_vault.cache.is_cached(TEST_TYPE_1, TEST_OBJ_ID_1)
    assert swh_vault.cache.is_cached(TEST_TYPE_2, TEST_OBJ_ID_1)


def test_different_type_same_content(swh_vault):
    swh_vault.cache.add(TEST_TYPE_1, TEST_OBJ_ID_1, TEST_CONTENT_1)
    swh_vault.cache.add(TEST_TYPE_2, TEST_OBJ_ID_1, TEST_CONTENT_1)
    assert swh_vault.cache.get(TEST_TYPE_1, TEST_OBJ_ID_1) == \
        TEST_CONTENT_1
    assert swh_vault.cache.get(TEST_TYPE_2, TEST_OBJ_ID_1) == \
        TEST_CONTENT_1
    assert swh_vault.cache.is_cached(TEST_TYPE_1, TEST_OBJ_ID_1)
    assert swh_vault.cache.is_cached(TEST_TYPE_2, TEST_OBJ_ID_1)


def test_different_id_same_type(swh_vault):
    swh_vault.cache.add(TEST_TYPE_1, TEST_OBJ_ID_1, TEST_CONTENT_1)
    swh_vault.cache.add(TEST_TYPE_1, TEST_OBJ_ID_2, TEST_CONTENT_2)
    assert swh_vault.cache.get(TEST_TYPE_1, TEST_OBJ_ID_1) == \
        TEST_CONTENT_1
    assert swh_vault.cache.get(TEST_TYPE_1, TEST_OBJ_ID_2) == \
        TEST_CONTENT_2
    assert swh_vault.cache.is_cached(TEST_TYPE_1, TEST_OBJ_ID_1)
    assert swh_vault.cache.is_cached(TEST_TYPE_1, TEST_OBJ_ID_2)
