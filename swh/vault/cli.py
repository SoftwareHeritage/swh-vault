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

    from swh.model.swhids import CoreSWHID


class SwhidParamType(click.ParamType):
    name = "swhid"

    def convert(self, value, param, ctx):
        from swh.model.exceptions import ValidationError
        from swh.model.swhids import CoreSWHID

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
    type=click.Path(
        exists=True,
        dir_okay=False,
    ),
    help="Configuration file.",
)
@click.argument("swhid", type=SwhidParamType())
@click.argument("outfile", type=click.File("wb"))
@click.option(
    "--bundle-type",
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
    bundle_type: Optional[str],
):
    """
    Runs a vault cooker for a single object (identified by a SWHID),
    and outputs it to the given file.
    """
    from swh.core import config
    from swh.model.swhids import ObjectType
    from swh.objstorage.exc import ObjNotFoundError
    from swh.objstorage.factory import get_objstorage
    from swh.storage import get_storage

    from .cookers import get_cooker_cls
    from .in_memory_backend import InMemoryVaultBackend

    conf = config.read(config_file)

    try:
        from swh.graph.http_client import RemoteGraphClient  # optional dependency

        graph = RemoteGraphClient(**conf["graph"]) if conf.get("graph") else None
    except ModuleNotFoundError:
        if conf.get("graph"):
            raise EnvironmentError(
                "Graph configuration required but module is not installed."
            )
        else:
            graph = None

    backend = InMemoryVaultBackend()

    if bundle_type is None:
        if swhid.object_type in (
            ObjectType.RELEASE,
            ObjectType.SNAPSHOT,
        ):
            bundle_type = "git_bare"
        elif swhid.object_type in (ObjectType.DIRECTORY,):
            bundle_type = "flat"
        else:
            raise click.ClickException(
                "No default bundle type for this kind of object, "
                "use --bundle-type to choose one"
            )

    try:
        cooker_cls = get_cooker_cls(bundle_type, swhid.object_type)
    except ValueError as e:
        raise click.ClickException(*e.args)

    storage = get_storage(**conf["storage"])
    objstorage = get_objstorage(**conf["objstorage"]) if "objstorage" in conf else None
    cooker = cooker_cls(
        swhid=swhid,
        backend=backend,
        storage=storage,
        graph=graph,
        objstorage=objstorage,
        max_bundle_size=None,  # No need for a size limit, we are running locally
    )
    cooker.cook()

    try:
        bundle = backend.fetch(cooker_cls.BUNDLE_TYPE, swhid)
    except ObjNotFoundError:
        bundle = None
    if bundle is None:
        import pdb

        pdb.set_trace()
        raise click.ClickException("Cooker did not write a bundle to the backend.")
    outfile.write(bundle)


@vault.command(name="rpc-serve")
@click.option(
    "--config-file",
    "-C",
    default=None,
    metavar="CONFIGFILE",
    type=click.Path(
        exists=True,
        dir_okay=False,
    ),
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
    from swh.vault.api.server import make_app_from_configfile

    ctx.ensure_object(dict)
    if "log_level" in ctx.obj:
        logging.getLogger("werkzeug").setLevel(ctx.obj["log_level"])

    try:
        app = make_app_from_configfile(config_file, debug=debug)
    except EnvironmentError as e:
        click.echo(e.msg, err=True)
        ctx.exit(1)

    app.run(host, port=int(port), debug=debug)


def main():
    logging.basicConfig()
    return serve(auto_envvar_prefix="SWH_VAULT")


if __name__ == "__main__":
    main()
