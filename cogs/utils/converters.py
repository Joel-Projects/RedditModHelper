from discord.ext import commands
from discord.ext.commands import MemberNotFound


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


class UserIDConverter(commands.Converter):
    async def convert(self, context, argument):
        try:
            member = await commands.MemberConverter().convert(context, argument)
            user_id = member.id
        except MemberNotFound:
            if argument.isdigit() and int(argument) < 2147483647:
                user_id = int(argument)
            else:
                return NotFound(argument)
        user = await context.cog.get_member(context.guild, user_id, return_member=True)
        return user or NotFound(argument)
