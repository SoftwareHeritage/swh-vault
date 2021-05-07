# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import logging
from typing import TYPE_CHECKING, Optional

import click

from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup
from swh.core.cli import swh as swh_cli_group

if TYPE_CHECKING:
    import io

    from swh.model.identifiers import CoreSWHID


class SwhidParamType(click.ParamType):
    name = "swhid"

    def convert(self, value, param, ctx):
        from swh.model.exceptions import ValidationError
        from swh.model.identifiers import CoreSWHID

        try:
            return CoreSWHID.from_string(value)
        except ValidationError:
            self.fail(f"expected core SWHID, got {value!r}", param, ctx)


@swh_cli_group.group(name="vault", context_settings=CONTEXT_SETTINGS, cls=AliasedGroup)
@click.pass_context
def vault(ctx):
    """Software Heritage Vault tools."""


@vault.command()
@click.option(
    "--config-file",
    "-C",
    default=None,
    metavar="CONFIGFILE",
    type=click.Path(exists=True, dir_okay=False,),
    help="Configuration file.",
)
@click.argument("swhid", type=SwhidParamType())
@click.argument("outfile", type=click.File("wb"))
@click.option(
    "--cooker-type",
    type=click.Choice(["flat", "gitfast", "git_bare"]),
    help="Selects which cooker to use, when there is more than one available "
    "for the given object type.",
)
@click.pass_context
def cook(
    ctx,
    config_file: str,
    swhid: CoreSWHID,
    outfile: io.RawIOBase,
    cooker_type: Optional[str],
):
    """
    Runs a vault cooker for a single object (identified by a SWHID),
    and outputs it to the given file.
    """
    from swh.core import config
    from swh.graph.client import RemoteGraphClient
    from swh.storage import get_storage

    from .cookers import COOKER_TYPES, get_cooker_cls
    from .in_memory_backend import InMemoryVaultBackend

    conf = config.read(config_file)

    supported_object_types = {name.split("_")[0] for name in COOKER_TYPES}
    if swhid.object_type.name.lower() not in supported_object_types:
        raise click.ClickException(
            f"No cooker available for {swhid.object_type.name} objects."
        )

    cooker_name = swhid.object_type.name.lower()

    if cooker_type:
        cooker_name = f"{cooker_name}_{cooker_type}"
        if cooker_name not in COOKER_TYPES:
            raise click.ClickException(
                f"{swhid.object_type.name.lower()} objects do not have "
                f"a {cooker_type} cooker."
            )
    else:
        if cooker_name not in COOKER_TYPES:
            raise click.ClickException(
                f"{swhid.object_type.name.lower()} objects need "
                f"an explicit --cooker-type."
            )

    backend = InMemoryVaultBackend()
    storage = get_storage(**conf["storage"])
    graph = RemoteGraphClient(**conf["graph"]) if "graph" in conf else None
    cooker_cls = get_cooker_cls(cooker_name)
    cooker = cooker_cls(cooker_name, swhid.object_id, backend, storage, graph)
    cooker.cook()

    bundle = backend.fetch(cooker_name, swhid.object_id)
    assert bundle, "Cooker did not write a bundle to the backend."
    outfile.write(bundle)


@vault.command(name="rpc-serve")
@click.option(
    "--config-file",
    "-C",
    default=None,
    metavar="CONFIGFILE",
    type=click.Path(exists=True, dir_okay=False,),
    help="Configuration file.",
)
@click.option(
    "--host",
    default="0.0.0.0",
    metavar="IP",
    show_default=True,
    help="Host ip address to bind the server on",
)
@click.option(
    "--port",
    default=5005,
    type=click.INT,
    metavar="PORT",
    help="Binding port of the server",
)
@click.option(
    "--debug/--no-debug",
    default=True,
    help="Indicates if the server should run in debug mode",
)
@click.pass_context
def serve(ctx, config_file, host, port, debug):
    """Software Heritage Vault RPC server."""
    import aiohttp

    from swh.vault.api.server import make_app_from_configfile

    ctx.ensure_object(dict)

    try:
        app = make_app_from_configfile(config_file, debug=debug)
    except EnvironmentError as e:
        click.echo(e.msg, err=True)
        ctx.exit(1)

    aiohttp.web.run_app(app, host=host, port=int(port))


def main():
    logging.basicConfig()
    return serve(auto_envvar_prefix="SWH_VAULT")


if __name__ == "__main__":
    main()
