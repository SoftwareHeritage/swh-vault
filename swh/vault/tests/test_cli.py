# Copyright (C) 2021 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import tempfile
from unittest.mock import MagicMock

import click
import click.testing
import pytest

from swh.model.identifiers import CoreSWHID
from swh.vault.cli import vault as vault_cli_group
from swh.vault.cookers.base import BaseVaultCooker
from swh.vault.in_memory_backend import InMemoryVaultBackend


def test_cook_unsupported_swhid():
    runner = click.testing.CliRunner()

    result = runner.invoke(vault_cli_group, ["cook", "swh:1:dir:f00b4r", "-"])
    assert isinstance(result.exception, SystemExit)
    assert "expected core SWHID" in result.stdout

    result = runner.invoke(vault_cli_group, ["cook", "swh:1:ori:" + "0" * 40, "-"])
    assert isinstance(result.exception, SystemExit)
    assert "expected core SWHID" in result.stdout


def test_cook_unknown_cooker():
    runner = click.testing.CliRunner()

    result = runner.invoke(
        vault_cli_group,
        ["cook", "swh:1:dir:" + "0" * 40, "-", "--bundle-type", "gitfast"],
    )
    assert isinstance(result.exception, SystemExit)
    assert "do not have a gitfast cooker" in result.stdout

    result = runner.invoke(vault_cli_group, ["cook", "swh:1:rev:" + "0" * 40, "-"])
    assert isinstance(result.exception, SystemExit)
    assert "use --bundle-type" in result.stdout


@pytest.mark.parametrize(
    "bundle_type,cooker_name_suffix,swhid_type",
    [("directory", "", "dir"), ("revision", "gitfast", "rev"),],
)
def test_cook_directory(bundle_type, cooker_name_suffix, swhid_type, mocker):
    storage = object()
    mocker.patch("swh.storage.get_storage", return_value=storage)

    backend = MagicMock(spec=InMemoryVaultBackend)
    backend.fetch.return_value = b"bundle content"
    mocker.patch(
        "swh.vault.in_memory_backend.InMemoryVaultBackend", return_value=backend
    )

    cooker = MagicMock(spec=BaseVaultCooker)
    cooker_cls = MagicMock(return_value=cooker)
    mocker.patch("swh.vault.cookers.get_cooker_cls", return_value=cooker_cls)

    runner = click.testing.CliRunner()

    swhid = CoreSWHID.from_string(f"swh:1:{swhid_type}:{'0'*40}")

    with tempfile.NamedTemporaryFile("a", suffix=".yml") as config_fd:
        config_fd.write('{"storage": {}}')
        config_fd.seek(0)
        if cooker_name_suffix:
            result = runner.invoke(
                vault_cli_group,
                [
                    "cook",
                    f"swh:1:{swhid_type}:{'0'*40}",
                    "-",
                    "-C",
                    config_fd.name,
                    "--bundle-type",
                    cooker_name_suffix,
                ],
            )
        else:
            result = runner.invoke(
                vault_cli_group, ["cook", str(swhid), "-", "-C", config_fd.name],
            )

    if result.exception is not None:
        raise result.exception

    cooker_cls.assert_called_once_with(
        swhid=swhid,
        backend=backend,
        storage=storage,
        graph=None,
        objstorage=None,
        max_bundle_size=None,
    )
    cooker.cook.assert_called_once_with()

    assert result.stdout_bytes == b"bundle content"
