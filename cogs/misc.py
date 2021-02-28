from .utils.commandCog import CommandCog


class Misc(CommandCog):
    """
    A collection of Miscellaneous commands
    """


def setup(bot):
    bot.add_cog(Misc(bot))
