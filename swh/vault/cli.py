import os
import logging

import click

from swh.core.config import SWH_CONFIG_DIRECTORIES, SWH_CONFIG_EXTENSIONS
from swh.core.cli import CONTEXT_SETTINGS, AliasedGroup

from swh.vault.api.server import make_app_from_configfile, DEFAULT_CONFIG_PATH

CFG_HELP = """Software Heritage Vault RPC server.

If the CONFIGFILE option is not set, the default config file search will
be used; first the SWH_CONFIG_FILENAME environment variable will be
checked, then the config file will be searched in:

%s""" % ('\n\n'.join('- %s(%s)' % (
    os.path.join(d, DEFAULT_CONFIG_PATH), '|'.join(SWH_CONFIG_EXTENSIONS))
                     for d in SWH_CONFIG_DIRECTORIES))


@click.group(name='vault', context_settings=CONTEXT_SETTINGS,
             cls=AliasedGroup)
@click.pass_context
def vault(ctx):
    '''Software Heritage Vault tools.'''
    pass


@vault.command(name='rpc-serve', help=CFG_HELP)
@click.option('--config-file', '-C', default=None,
              metavar='CONFIGFILE',
              type=click.Path(exists=True, dir_okay=False,),
              help="Configuration file.")
@click.option('--no-stdout', is_flag=True, default=False,
              help="Do NOT output logs on the console")
@click.option('--host', default='0.0.0.0',
              metavar='IP', show_default=True,
              help="Host ip address to bind the server on")
@click.option('--port', default=5005, type=click.INT,
              metavar='PORT',
              help="Binding port of the server")
@click.option('--debug/--no-debug', default=True,
              help="Indicates if the server should run in debug mode")
@click.pass_context
def serve(ctx, config_file, no_stdout, host, port, debug):
    import aiohttp
    from swh.scheduler.celery_backend.config import setup_log_handler

    ctx.ensure_object(dict)
    setup_log_handler(
        loglevel=ctx.obj.get('log_level', logging.INFO), colorize=False,
        format='[%(levelname)s] %(name)s -- %(message)s',
        log_console=not no_stdout)

    try:
        app = make_app_from_configfile(config_file, debug=debug)
    except EnvironmentError as e:
        click.echo(e.msg, err=True)
        ctx.exit(1)

    aiohttp.web.run_app(app, host=host, port=int(port))


def main():
    logging.basicConfig()
    return serve(auto_envvar_prefix='SWH_VAULT')


if __name__ == '__main__':
    main()
