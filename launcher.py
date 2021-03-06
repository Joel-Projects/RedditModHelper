#!/usr/bin/env python3
import asyncio
import contextlib
import importlib
import logging
import sys
import traceback

import click

import config
from bot import RedditModHelper, __version__, initial_extensions, log, services
from cogs.utils.db import Table

if sys.platform != "darwin":
    try:
        import uvloop
    except ImportError:
        pass
    else:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

pool = None
postgres = services._getDbConnectionSettings()


class RemoveNoise(logging.Filter):
    def __init__(self):
        super().__init__(name="discord.state")

    def filter(self, record):
        if record.levelname == "WARNING" and "referencing an unknown" in record.msg:
            return False
        return True


@contextlib.contextmanager
def setup_logging():
    try:
        logging.getLogger("discord").setLevel(logging.ERROR)
        logging.getLogger("discord.http").setLevel(logging.ERROR)
        logging.getLogger("discord.gateway").setLevel(logging.ERROR)
        logging.getLogger("discord.state").setLevel(logging.ERROR)
        logging.getLogger("discord_slash").setLevel(0)
        logging.getLogger("discord_slash.client").setLevel(0)
        log.info(f"Starting {config.bot_name} | Version: {__version__}")
        yield
    finally:
        handlers = log.handlers[:]
        for handler in handlers:
            handler.close()
            log.removeHandler(handler)


def run_bot():
    loop = asyncio.get_event_loop()
    if sys.platform == "darwin":
        try:
            import nest_asyncio

            nest_asyncio.apply(loop)
        except ImportError:
            pass
    kwargs = {
        "command_timeout": 300,
        "max_size": 20,
        "min_size": 20,
    }
    try:
        pool = loop.run_until_complete(Table.create_pool(**postgres, **kwargs))
    except Exception as error:
        click.echo("Could not set up PostgreSQL. Exiting.", file=sys.stderr)
        log.exception(f"Could not set up PostgreSQL: {error}")
        return
    bot = RedditModHelper(pool)
    bot.run()


@click.group(invoke_without_command=True, options_metavar="[options]")
@click.pass_context
def main(context):
    """Launches the bot."""
    if context.invoked_subcommand is None:
        with setup_logging():
            run_bot()


@main.group(short_help="database stuff", options_metavar="[options]")
def db():
    pass


@db.command(short_help="initializes the databases for the bot", options_metavar="[options]")
@click.argument("cogs", nargs=-1, metavar="[cogs]")
@click.option("-q", "--quiet", help="less verbose output", is_flag=True)
def init(cogs, quiet):
    """This manages the migrations and database creation system for you."""

    run = asyncio.get_event_loop().run_until_complete
    try:
        pool = run(Table.create_pool(**postgres))
    except Exception:
        click.echo(
            f"Could not create PostgreSQL connection pool.\n{traceback.format_exc()}",
            err=True,
        )
        log.critical(
            f"Could not create PostgreSQL connection pool.\n{traceback.format_exc()}",
        )
        return

    if not cogs:
        cogs = initial_extensions
    else:
        cogs = [f"cogs.{e}" if not e.startswith("cogs.") else e for e in cogs]

    for ext in cogs:
        try:
            importlib.import_module(ext)
        except Exception:
            click.echo(f"Could not load {ext}.\n{traceback.format_exc()}", err=True)
            return

    for table in Table.all_tables():
        try:
            created = run(table.create(verbose=not quiet, run_migrations=False))
        except Exception:
            click.echo(
                f"Could not create {table.__tablename__}.\n{traceback.format_exc()}",
                err=True,
            )
        else:
            if created:
                click.echo(f"[{table.__module__}] Created {table.__tablename__}.")
            else:
                click.echo(f"[{table.__module__}] No work needed for {table.__tablename__}.")


@db.command(short_help="migrates the databases")
@click.argument("cog", nargs=1, metavar="[cog]")
@click.option("-q", "--quiet", help="less verbose output", is_flag=True)
@click.pass_context
def migrate(context, cog, quiet):
    """Update the migration file with the newest schema."""

    if not cog.startswith("cogs."):
        cog = f"cogs.{cog}"

    try:
        importlib.import_module(cog)
    except Exception:
        click.echo(f"Could not load {cog}.\n{traceback.format_exc()}", err=True)
        return

    def work(table, *, invoked=False):
        try:
            actually_migrated = table.write_migration()
        except RuntimeError as e:
            click.echo(f"Could not migrate {table.__tablename__}: {e}", err=True)
            if not invoked:
                click.confirm("do you want to create the table?", abort=True)
                context.invoke(init, cogs=[cog], quiet=quiet)
                work(table, invoked=True)
            sys.exit(-1)
        else:
            if actually_migrated:
                click.echo(f"Successfully updated migrations for {table.__tablename__}.")
            else:
                click.echo(f"Found no changes for {table.__tablename__}.")

    for table in Table.all_tables():
        work(table)

    click.echo(f"Done migrating {cog}.")


async def apply_migration(cog, quiet, index, *, downgrade=False):
    try:
        pool = await Table.create_pool(**postgres)
    except Exception:
        click.echo(
            f"Could not create PostgreSQL connection pool.\n{traceback.format_exc()}",
            err=True,
        )
        return

    if not cog.startswith("cogs."):
        cog = f"cogs.{cog}"

    try:
        importlib.import_module(cog)
    except Exception:
        click.echo(f"Could not load {cog}.\n{traceback.format_exc()}", err=True)
        return

    async with pool.acquire() as con:
        tr = con.transaction()
        await tr.start()
        for table in Table.all_tables():
            try:
                await table.migrate(index=index, downgrade=downgrade, verbose=not quiet, connection=con)
            except RuntimeError as e:
                click.echo(f"Could not migrate {table.__tablename__}: {e}", err=True)
                await tr.rollback()
                break
        else:
            await tr.commit()


@db.command(short_help="upgrades from a migration")
@click.argument("cog", nargs=1, metavar="[cog]")
@click.option("-q", "--quiet", help="less verbose output", is_flag=True)
@click.option("--index", help="the index to use", default=-1)
def upgrade(cog, quiet, index):
    """Runs an upgrade from a migration"""
    run = asyncio.get_event_loop().run_until_complete
    run(apply_migration(cog, quiet, index))


@db.command(short_help="downgrades from a migration")
@click.argument("cog", nargs=1, metavar="[cog]")
@click.option("-q", "--quiet", help="less verbose output", is_flag=True)
@click.option("--index", help="the index to use", default=-1)
def downgrade(cog, quiet, index):
    """Runs an downgrade from a migration"""
    run = asyncio.get_event_loop().run_until_complete
    run(apply_migration(cog, quiet, index, downgrade=True))


async def remove_databases(pool, cog, quiet):
    async with pool.acquire() as con:
        tr = con.transaction()
        await tr.start()
        for table in Table.all_tables():
            try:
                await table.drop(verbose=not quiet, connection=con)
            except RuntimeError as e:
                click.echo(f"Could not drop {table.__tablename__}: {e}", err=True)
                await tr.rollback()
                break
            else:
                click.echo(f"Dropped {table.__tablename__}.")
        else:
            await tr.commit()
            click.echo(f"successfully removed {cog} tables.")


@db.command(short_help="removes a cog's table", options_metavar="[options]")
@click.argument("cog", metavar="<cog>")
@click.option("-q", "--quiet", help="less verbose output", is_flag=True)
def drop(cog, quiet):
    """This removes a database and all its migrations.

    You must be pretty sure about this before you do it,
    as once you do it there's no coming back.

    Also note that the name must be the database name, not
    the cog name.
    """

    run = asyncio.get_event_loop().run_until_complete
    click.confirm("do you really want to do this?", abort=True)

    try:
        pool = run(Table.create_pool(**postgres))
    except Exception:
        click.echo(
            f"Could not create PostgreSQL connection pool.\n{traceback.format_exc()}",
            err=True,
        )
        return

    if not cog.startswith("cogs."):
        cog = f"cogs.{cog}"

    try:
        importlib.import_module(cog)
    except Exception:
        click.echo(f"Could not load {cog}.\n{traceback.format_exc()}", err=True)
        return

    run(remove_databases(pool, cog, quiet))


if __name__ == "__main__":
    try:
        main()
    except (SystemExit, KeyboardInterrupt):
        print("Exiting, closing db connections")
        if pool:
            pool.close()
        exit()
    except Exception as error:
        log.exception(error)
        print("Exiting, closing db connections")
        if pool:
            pool.close()
        exit()
