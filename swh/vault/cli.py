import logging
import click
import aiohttp

from swh.vault.api.server import make_app_from_configfile


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('--config-file', '-C', default=None,
              type=click.Path(exists=True, dir_okay=False,),
              help="Configuration file.")
@click.option('--log-level', '-l', default='INFO',
              type=click.Choice(logging._nameToLevel.keys()),
              help="Log level (default to INFO)")
@click.option('--no-stdout', is_flag=True, default=False,
              help="Do NOT output logs on the console")
@click.option('--host', default='0.0.0.0', help="Host to run the server")
@click.option('--port', default=5005, type=click.INT,
              help="Binding port of the server")
@click.option('--debug/--nodebug', default=True,
              help="Indicates if the server should run in debug mode")
@click.pass_context
def cli(ctx, config_file, log_level, no_stdout, host, port, debug):
    """Software Heritage Vault API server

    """
    from swh.scheduler.celery_backend.config import setup_log_handler
    log_level = setup_log_handler(
        loglevel=log_level, colorize=False,
        format='[%(levelname)s] %(name)s -- %(message)s',
        log_console=not no_stdout)

    try:
        app = make_app_from_configfile(config_file, debug=debug)
    except EnvironmentError as e:
        click.echo(e.msg, err=True)
        ctx.exit(1)

    aiohttp.web.run_app(app, host=host, port=int(port))


def main():
    return cli(auto_envvar_prefix='SWH_VAULT')


if __name__ == '__main__':
    main()
