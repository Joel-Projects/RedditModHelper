import asyncio

from discord import Embed
from discord_slash.cog_ext import cog_slash
from discord_slash.utils.manage_commands import create_option

from .utils.command_cog import CommandCog


class Misc(CommandCog):
    """A collection of Miscellaneous commands."""

    # @cog_slash(options=[create_option("test", "test", 9, True)])
    # async def ping(self, context):
    #     response = await context.defer()
    #     print()


def setup(bot):
    bot.add_cog(Misc(bot))
