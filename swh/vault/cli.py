# Copyright (C) 2015-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import logging

import click

from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup
from swh.core.cli import swh as swh_cli_group

CFG_HELP = """Software Heritage Vault RPC server."""


@swh_cli_group.group(name="vault", context_settings=CONTEXT_SETTINGS, cls=AliasedGroup)
@click.pass_context
def vault(ctx):
    """Software Heritage Vault tools."""
    pass


@vault.command(name="rpc-serve", help=CFG_HELP)
@click.option(
    "--config-file",
    "-C",
    default=None,
    metavar="CONFIGFILE",
    type=click.Path(exists=True, dir_okay=False,),
    help="Configuration file.",
)
@click.option(
    "--no-stdout", is_flag=True, default=False, help="Do NOT output logs on the console"
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
def serve(ctx, config_file, no_stdout, host, port, debug):
    import aiohttp

    from swh.scheduler.celery_backend.config import setup_log_handler
    from swh.vault.api.server import make_app_from_configfile

    ctx.ensure_object(dict)
    setup_log_handler(
        loglevel=ctx.obj.get("log_level", logging.INFO),
        colorize=False,
        format="[%(levelname)s] %(name)s -- %(message)s",
        log_console=not no_stdout,
    )

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
