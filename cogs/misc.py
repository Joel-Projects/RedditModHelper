import asyncio

from discord import Embed
from discord_slash.cog_ext import cog_slash
from discord_slash.utils.manage_commands import create_option

from .utils.command_cog import CommandCog
from .utils.integration_context import SlashContext


class Misc(CommandCog):
    """A collection of Miscellaneous commands."""

    #
    # @cog_slash(
    #     options=[
    #         create_option("hidden", "Whether the prompt is hidden.", bool, False),
    #         create_option("delete_after", "Whether to delete the prompt afterwards.", bool, False),
    #     ]
    # )
    # async def test_prompt(self, context: SlashContext, hidden=False, delete_after=False):
    #     await context.defer(hidden=hidden)
    #     response = await context.prompt("Does this work?", delete_after=delete_after, hidden=hidden)
    #     print(response)


def setup(bot):
    bot.add_cog(Misc(bot))
