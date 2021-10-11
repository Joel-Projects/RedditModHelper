from discord.ext import commands
from discord.ext.commands import UserNotFound


class RedditorConverter(commands.Converter):
    async def convert(self, context, argument):
        redditor = await context.cog.get_mod(context, argument)
        if redditor:
            return redditor


class SubredditConverter(commands.Converter):
    async def convert(self, context, argument):
        subreddit = await context.cog.get_sub(context, argument)
        if subreddit:
            return subreddit


class NotFound:
    def __init__(self, arg):
        self.arg = arg
