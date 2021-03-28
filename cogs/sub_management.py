import time
from subprocess import CalledProcessError, check_call

import asyncpraw
import credmgr
import discord
import praw
from discord import Embed
from discord_slash.utils.manage_commands import create_option

from .utils import db
from .utils.command_cog import CommandCog
from .utils.converters import RedditorConverter, SubredditConverter
from .utils.slash import cog_slash, cog_subcommand
from .utils.utils import parse_sql


class Subreddits(db.Table, table_name="subreddits"):
    name = db.Column(db.String, primary_key=True, unique=True, nullable=False)
    mod_role = db.Column(db.Integer(big=True), nullable=False)
    channel_id = db.Column(db.Integer(big=True), nullable=False)
    modlog_account = db.Column(db.String, nullable=False)
    alert_channel_id = db.Column(db.Integer(big=True))


class Webhooks(db.Table, table_name="webhooks"):
    subreddit = db.Column(db.String, primary_key=True, unique=True, nullable=False)
    admin_webhook = db.Column(db.String)
    alert_webhook = db.Column(db.String)


class SubredditManagement(CommandCog):
    """A collection of Subreddit Management commands."""

    async def generate_subreddit_embed(
        self,
        action,
        subreddit,
        channel=None,
        mod_role=None,
        mod_account=None,
        alert_channel=None,
        title=None,
        description=None,
        url=None,
        result=None,
    ):
        if result:
            channel = self.bot.get_channel(result.channel_id)
            mod_role = self.bot.snoo_guild.get_role(result.mod_role_id)
            mod_account = result.modlog_account
            alert_channel = self.bot.get_channel(result.alert_channel_id) if result.alert_channel_id else None
        embed = Embed(title=title or "Confirmation", color=discord.Color.green())
        if description:
            embed.description = f"Successfully {action} [r/{subreddit}](https://www.reddit.com/r/{subreddit})!"
        if url:
            embed.url = url
        sub = await self.reddit.subreddit(subreddit, fetch=True)
        if sub.icon_img:
            embed.set_thumbnail(url=sub.icon_img)
        embed.add_field(name="Mod Role", value=mod_role.mention)
        embed.add_field(name="Mod Channel", value=channel.mention)
        embed.add_field(
            name="Mod Account",
            value=f"[u/{mod_account}](https://www.reddit.com/user/{mod_account})" if mod_account else "*Not Set*",
        )
        embed.add_field(name="Alert Channel", value=alert_channel.mention if alert_channel else "*Not Set*")
        embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
        return embed

    @cog_slash(
        options=[
            create_option("subreddit", "Subreddit to add.", str, True),
            create_option("mod_role", "Mod role for this subreddit.", discord.Role, True),
            create_option("channel", "That subreddit's mod chat or bot command channel.", discord.TextChannel, True),
            create_option(
                "mod_account",
                "Mod account to use. This is required for creating matrices, counting the queue, and getting alerts.",
                str,
                True,
            ),
            create_option("alert_channel", "That subreddit's alert channel.", discord.TextChannel, False),
        ]
    )
    async def add_sub(
        self,
        context,
        subreddit,
        mod_role: discord.Role,
        channel: discord.TextChannel,
        mod_account,
        alert_channel: discord.TextChannel = None,
    ):
        """Adds a subreddit to the bot. If you need help or have questions, contact Lil_SpazJoekp."""
        await context.defer()
        subreddit = await SubredditConverter().convert(context, subreddit)
        mod_account = await RedditorConverter().convert(context, mod_account)
        if None in [subreddit, mod_account]:
            return
        results = parse_sql(await self.sql.fetch("SELECT * FROM subreddits WHERE name=$1", subreddit))
        if results:
            await self.error_embed(context, f"r/{subreddit} is already added. Please use `/manage_sub update` instead.")
            return
        required_scopes = ["identity", "modlog", "mysubreddits", "read", "modposts"]
        try:
            reddit: praw.Reddit = self.bot.credmgr_bot.redditApp.reddit(mod_account)
            current_scopes = reddit.auth.scopes()
            if not set(required_scopes).issubset(current_scopes) and "*" not in current_scopes:
                auth_url = self.bot.credmgr_bot.redditApp.genAuthUrl(required_scopes, True)
                confirm = await context.prompt(
                    f"My authorization for u/{mod_account} is not valid. I will need you to reauthorize me using this link:\n{auth_url}.\n\nOnce you are done, please confirm below.\n\nIf you have any questions, please contact <@393801572858986496>.",
                    delete_after=False,
                    timeout=None,
                )
                if not confirm:
                    await context.send("Cancelled")
        except credmgr.exceptions.NotFound:
            auth_url = self.bot.credmgr_bot.redditApp.genAuthUrl(required_scopes, True)
            confirm = await context.prompt(
                f"u/{mod_account} has not granted me permission yet, I will need you to authorize me using this link:\n{auth_url}.\n\nOnce you are done, please confirm below.\n\nIf you have any questions, please contact <@393801572858986496>.",
                delete_after=False,
                timeout=None,
            )
            if not confirm:
                await context.send("Cancelled")
        except Exception as error:
            self.log.exception(error)
            await self.error_embed(context, f"Failed to add r/{subreddit}.")
        if not await self.verify_valid_auth(context, mod_account, required_scopes):
            return
        sub: asyncpraw.reddit.Subreddit = await self.reddit.subreddit(subreddit)
        moderator = await sub.moderator(mod_account)
        if moderator:
            moderator = moderator[0]
        if all(perm not in moderator.mod_permissions for perm in ["all", "posts"]):
            await self.error_embed(
                context,
                f"u/{mod_account} does not have enough permissions. Please ensure they have at least `posts` permissions and try again.\n\nIf you have any questions, please contact <@393801572858986496>.",
            )
            return
        if alert_channel:
            await self.create_or_update_alert_channel(context, subreddit, alert_channel)
        try:
            await self.sql.execute(
                "INSERT INTO subreddits (name, role_id, channel_id, modlog_account, alert_channel_id) VALUES ($1, $2, $3, $4, $5)",
                subreddit,
                mod_role.id,
                channel.id,
                mod_account,
                alert_channel.id if alert_channel else alert_channel,
            )
            if not self.bot.debug:
                try:
                    check_call(["pm2", "restart", "RedditModHelper-Stream"])
                except CalledProcessError as error:
                    self.log.exception(error)
            embed = await self.generate_subreddit_embed(
                "added", subreddit, channel, mod_role, mod_account, alert_channel
            )
            await context.send(embed=embed)
        except Exception as error:
            self.log.exception(error)
            await self.error_embed(context, f"Failed to add r/{subreddit}.")

    @cog_subcommand(
        base="manage_sub",
        options=[
            create_option("subreddit", "Subreddit to remove.", str, True),
        ],
    )
    async def delete(
        self,
        context,
        subreddit,
    ):
        """Removes a subreddit from the bot."""
        await context.defer()
        subreddit = await SubredditConverter().convert(context, subreddit)
        if subreddit is None:
            return
        results = parse_sql(await self.sql.fetch("SELECT * FROM subreddits WHERE name=$1", subreddit))
        if results:
            result = results[0]
            authorized_roles = await context.cog.get_bot_config("authorized_roles")
            is_authorized = any([role.id in authorized_roles for role in context.author.roles])
            if result.channel_id == context.channel_id or is_authorized:
                confirm = await context.prompt(
                    f"Are you *sure* you want to delete r/{subreddit} from this bot?",
                )
                if not confirm:
                    return
                try:
                    await self.sql.execute(
                        "INSERT INTO deleted_subreddits (name, role_id, channel_id, modlog_account, alert_channel_id) VALUES ($1, $2, $3, $4, $5)",
                        subreddit,
                        result.role_id,
                        result.channel_id,
                        result.modlog_account,
                        result.alert_channel_id,
                    )
                    await self.sql.execute("DELETE FROM subreddits WHERE name=$1", subreddit)
                    await self.sql.execute("DELETE FROM webhooks WHERE subreddit=$1", subreddit)
                    alert_channel = self.bot.get_channel(result.alert_channel_id)
                    if alert_channel:
                        channel_webhooks = await alert_channel.webhooks()
                        if channel_webhooks:
                            for webhook in channel_webhooks:
                                try:
                                    await webhook.delete()
                                except Exception:
                                    pass
                    embed = await self.generate_subreddit_embed("deleted", subreddit, result=result)
                    await context.send(embed=embed)
                except Exception as error:
                    self.log.exception(error)
                    await self.error_embed(
                        context, f"Failed to delete r/{subreddit}. Contact <@393801572858986496> for more help."
                    )
            else:
                await self.error_embed(
                    context, f"You must execute this command from that subreddit's channel (<#{result.channel_id}>)."
                )
        else:
            await self.error_embed(context, f"r/{subreddit} has not been added to this bot.")

    #
    # @cog_subcommand(base='manage_sub', subcommand_group='update', options=[create_option("subreddit", "Subreddit to update.", str, True), create_option("mod_role", "New mod role.", discord.Role, True)])
    # async def mod_role(self, context, subreddit, mod_role: discord.Role):
    #     """Update subreddit mod role."""
    #     print()
    #     subreddit = await SubredditConverter().convert(context, subreddit)
    #
    # @cog_subcommand(base='manage_sub', subcommand_group='update', options=[create_option("subreddit", "Subreddit to update.", str, True), create_option("channel", "New mod channel.", discord.TextChannel, True)])
    # async def channel(self, context, subreddit, channel: discord.TextChannel):
    #     """Update subreddit mod channel."""
    #     print()
    #     subreddit = await SubredditConverter().convert(context, subreddit)
    #
    # @cog_subcommand(base='manage_sub', subcommand_group='update', options=[create_option("subreddit", "Subreddit to update.", str, True), create_option("mod_channel", "New mod mod account.", str, True)])
    # async def mod_account(self, context, subreddit, mod_account):
    #     """Update subreddit mod account."""
    #     print()
    #     subreddit = await SubredditConverter().convert(context, subreddit)
    #     mod_account = await RedditorConverter().convert(context, mod_account)
    #
    # @cog_subcommand(base='manage_sub', subcommand_group='update', options=[create_option("subreddit", "Subreddit to update.", str, True), create_option("alert_channel", "New mod alert channel. Execute without any channel to clear.", discord.TextChannel, False)])
    # async def alert_channel(self, context, subreddit, alert_channel: discord.TextChannel):
    #     """Update subreddit mod alert channel."""
    #     await context.defer()
    #     subreddit = await SubredditConverter().convert(context, subreddit)
    #     results = parse_sql(await self.sql.fetch('SELECT * FROM subreddits WHERE name=$1', subreddit))
    #     if results:
    #         result = results[0]
    #         old_alert_channel = self.bot.get_channel(result.alert_channel_id)
    #         if old_alert_channel:
    #             if old_alert_channel != alert_channel:
    #                 channel_webhooks = await old_alert_channel.webhooks()
    #                 if channel_webhooks:
    #                     for webhook in channel_webhooks:
    #                         try:
    #                             await webhook.delete()
    #                         except Exception:
    #                             pass
    #             else:
    #                 return
    #         if alert_channel:
    #             results = parse_sql(await self.sql.fetch('UPDATE subreddits SET alert_channel_id=$1 WHERE name=$2 RETURNING *', alert_channel.id, subreddit))
    #             await self.create_or_update_alert_channel(context, subreddit, alert_channel)
    #         else:
    #             results = parse_sql(await self.sql.fetch('UPDATE subreddits SET alert_channel_id=$1 WHERE name=$2 RETURNING *', alert_channel, subreddit))
    #             await self.sql.execute('DELETE FROM webhooks WHERE subreddit=$1', subreddit)
    #         result = results[0]
    #         embed = await self.generate_subreddit_embed('updated', subreddit, result=result)
    #         await context.send(embed=embed)
    #     else:
    #         await self.error_embed(context, f"r/{subreddit} has not been added to this bot.")

    @cog_subcommand(base="manage_sub", options=[create_option("subreddit", "Subreddit to update.", str, True)])
    async def view(self, context, subreddit):
        """View a subreddit."""
        await context.defer()
        subreddit = await SubredditConverter().convert(context, subreddit)
        if subreddit is None:
            return
        results = parse_sql(await self.sql.fetch("SELECT * FROM subreddits WHERE name=$1", subreddit))
        if results:
            result = results[0]
            embed = await self.generate_subreddit_embed(
                None, subreddit, result=result, title=f"r/{subreddit}", url=f"https://www.reddit.com/r/{subreddit}"
            )
            await context.send(embed=embed)
        else:
            await self.error_embed(context, f"r/{subreddit} has not been added to this bot.")

    async def create_or_update_alert_channel(self, context, subreddit, alert_channel):
        with open("redditadmin.png", "rb") as file:
            admin_avatar = file.read()
        sub = await self.reddit.subreddit(subreddit, fetch=True)
        if sub.icon_img:
            try:
                response = await self.bot.session.get(sub.icon_img)
                mod_avatar = await response.read()
            except Exception:
                with open("redditmod.png", "rb") as file:
                    mod_avatar = file.read()
        mapping = {"admin_webhook": "Admin Action Alert", "alert_webhook": "Subreddit Alert"}
        webhook_names = ["admin_webhook", "alert_webhook"]
        results = parse_sql(await self.sql.fetch("SELECT * FROM webhooks WHERE subreddit=$1", subreddit))
        webhooks = {}
        channel_webhooks = await alert_channel.webhooks()
        if results:
            result = results[0]
            for webhook_name in webhook_names:
                webhook_url = getattr(result, webhook_name)
                if webhook_url:
                    webhook = discord.Webhook.from_url(
                        url=webhook_url, adapter=discord.AsyncWebhookAdapter(self.bot.session)
                    )
                    if webhook not in channel_webhooks:
                        webhooks[webhook_name] = await alert_channel.create_webhook(
                            name=mapping[webhook_name],
                            avatar=mod_avatar if webhook_name == "alert_webhook" else admin_avatar,
                        )
                else:
                    webhooks[webhook_name] = await alert_channel.create_webhook(
                        name=mapping[webhook_name],
                        avatar=mod_avatar if webhook_name == "alert_webhook" else admin_avatar,
                    )
            if webhooks:
                admin_webhook = webhooks.get("admin_webhook", None)
                if admin_webhook:
                    admin_webhook = admin_webhook.url
                alert_webhook = webhooks.get("alert_webhook", None)
                if alert_webhook:
                    alert_webhook = alert_webhook.url
                await self.sql.execute(
                    "UPDATE webhooks SET admin_webhook=$1, alert_webhook=$2 WHERE subreddit=$3",
                    admin_webhook,
                    alert_webhook,
                    subreddit,
                )
        else:
            admin_webhook = await alert_channel.create_webhook(name=mapping["admin_webhook"], avatar=admin_avatar)
            alert_webhook = await alert_channel.create_webhook(name=mapping["alert_webhook"], avatar=mod_avatar)
            await self.sql.execute(
                "INSERT INTO webhooks (subreddit, admin_webhook, alert_webhook) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                subreddit,
                admin_webhook.url,
                alert_webhook.url,
            )

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
