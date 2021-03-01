import discord

from .utils.commandCog import CommandCog
from .utils.commands import command


class SubredditManagement(CommandCog):
    """A collection of Subreddit Management commands."""

    @command(hidden=True)
    async def addsub(self, context, subreddit=None, channel: discord.TextChannel=None, mod_account=None, alert_channel: discord.TextChannel=None):
        """Adds a subreddit to the bot.

        Parameters:
            subreddit: Subreddit to add.
            channel: That subreddit's channel.
            mod_account: The moderator account for obtaining subreddit mod logs. Note: This to make generating mod matrices extremely faster.
            alert_channel: The channel where alerts will go to.

        """
        if not subreddit:
            subreddit = await context.ask('Subreddit?')
            subreddit = await self.validateSubreddit(context, subreddit)
        if not channel:
            channel = await context.ask(f"What is the channel for r/{subreddit}'s mod chat?")
        if not mod_account:
            mod_account = await context.ask(f"What account will be used for obtaining mod logs from r/{subreddit}?\nNote: This to make generating mod matrices extremely faster.")
        if not alert_channel:
            alert_channel = await context.ask(f"What channel will be used for sending various alerts for r/{subreddit}?")
        await self.sql.execute('INSERT INTO redditmodhelper.subreddits (name, channel_id, modlog_account, alert_channel_id) VALUES ($1, $2, $3, $4)', subreddit, channel.id, mod_account, alert_channel.id)
        await self.successEmbed(context, f'Successfully added r/{subreddit}!')

    async def validateSubreddit(self, context, subreddit):
        return await self.get_sub(context, subreddit)


def setup(bot):
    bot.add_cog(SubredditManagement(bot))