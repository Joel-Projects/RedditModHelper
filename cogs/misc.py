import asyncio

from discord import Embed

from .utils.command_cog import CommandCog
from .utils.slash import cog_slash


class Misc(CommandCog):
    """A collection of Miscellaneous commands."""

    # @cog_slash()
    # async def ping(self, context):
    #     response = await context.send(embed=Embed(title="test", description="test"))
    #     async with context.typing():
    #         await asyncio.sleep(10)
    #         await response.edit()
    #     print()


def setup(bot):
    bot.add_cog(Misc(bot))
