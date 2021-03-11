import credmgr
import discord
import praw

from .utils.command_cog import CommandCog
from .utils.commands import command
from .utils.converters import RedditorConverter, SubredditConverter
from .utils.utils import parse_sql


class SubredditManagement(CommandCog):
    """A collection of Subreddit Management commands."""

    @command(hidden=True)
    async def addsub(
        self,
        context,
        subreddit: SubredditConverter,
        mod_role: discord.Role,
        channel: discord.TextChannel,
        mod_account: RedditorConverter,
        alert_channel: discord.TextChannel = None,
    ):
        """Adds a subreddit to the bot.

        Parameters:

            subreddit: Subreddit to add.
            mod_role: That subreddit's mod role.
            channel: That subreddit's mod chat or bot command channel.
            mod_account: The moderator account for obtaining subreddit mod logs. Note: This is required to make generating mod matrices extremely faster and counting the mod queue. It needs to have at least `post` permissions.
            alert_channel: The channel where alerts will go to. (Optional)

        """
        if None in [subreddit, mod_account]:
            return
        results = parse_sql(await self.sql.fetch("SELECT * FROM subreddits WHERE name=$1", subreddit))
        if results:
            confirm = await context.prompt(
                f"r/{subreddit} is already added. Do you want to overwrite it?",
                delete_after=True,
            )
            if not confirm:
                return
        required_scopes = ["identity", "modlog", "mysubreddits", "read", "modposts"]
        try:
            reddit: praw.Reddit = self.bot.credmgr_bot.redditApp.reddit(mod_account)
            current_scopes = reddit.auth.scopes()
            if not set(required_scopes).issubset(current_scopes) and "*" not in current_scopes:
                auth_url = self.bot.credmgr_bot.redditApp.genAuthUrl(required_scopes, True)
                confirm = await context.prompt(
                    f"My authorization for u/{mod_account} is not valid. I will need you to reauthorize me using this link:\n{auth_url}.\n\nOnce you are done, please confirm below.\n\nIf you have any questions, please contact <@393801572858986496>.",
                    delete_after=True,
                )
                if not confirm:
                    await context.send("Cancelled")
        except credmgr.exceptions.NotFound:
            auth_url = self.bot.credmgr_bot.redditApp.genAuthUrl(required_scopes, True)
            confirm = await context.prompt(
                f"u/{mod_account} has not granted me permission yet, I will need you to reauthorize me using this link:\n{auth_url}.\n\nOnce you are done, please confirm below.\n\nIf you have any questions, please contact <@393801572858986496>.",
                delete_after=True,
            )
            if not confirm:
                await context.send("Cancelled")
                await self.error_embed(context, f"Failed to add r/{subreddit}.")
        except Exception as error:
            self.log.exception(error)
        if not await self.verify_valid_auth(context, mod_account, required_scopes):
            return
        if alert_channel:
            alert_channel = alert_channel.id
        try:
            await self.sql.execute(
                "INSERT INTO subreddits (name, role_id, channel_id, modlog_account, alert_channel_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (name) DO UPDATE SET role_id=EXCLUDED.role_id, channel_id=EXCLUDED.channel_id, modlog_account=EXCLUDED.modlog_account, alert_channel_id=EXCLUDED.alert_channel_id",
                subreddit,
                mod_role.id,
                channel.id,
                mod_account,
                alert_channel,
            )
            await self.success_embed(context, f"Successfully added r/{subreddit}!")
        except Exception as error:
            self.log.exception(error)
            await self.error_embed(context, f"Failed to add r/{subreddit}.")

    async def verify_valid_auth(self, context, mod_account, required_scopes):
        final_failed_message = "Authorization failed. Please try again or contact <@393801572858986496>."
        try:
            reddit: praw.Reddit = self.bot.credmgr_bot.redditApp.reddit(mod_account)
            current_scopes = reddit.auth.scopes()
            if not set(required_scopes).issubset(current_scopes) and "*" not in current_scopes:
                await self.error_embed(context, final_failed_message)
                return False
            else:
                return True
        except Exception:
            await self.error_embed(context, final_failed_message)
            return False


def setup(bot):
    bot.add_cog(SubredditManagement(bot))
