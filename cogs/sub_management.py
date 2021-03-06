import os
import time
from subprocess import CalledProcessError, check_call

import asyncpraw
import credmgr
import discord
import praw
from discord import Embed
from discord_slash.cog_ext import cog_slash, cog_subcommand
from discord_slash.utils.manage_commands import create_option

from .utils import db
from .utils.command_cog import CommandCog
from .utils.converters import RedditorConverter, SubredditConverter
from .utils.utils import EmbedType, generate_result_embed, parse_sql


class Subreddits(db.Table, table_name="subreddits"):
    name = db.Column(db.String, primary_key=True, unique=True, nullable=False)
    server_id = db.Column(db.Integer(big=True), nullable=False)
    mod_role = db.Column(db.Integer(big=True), nullable=False)
    channel_id = db.Column(db.Integer(big=True), nullable=False)
    modlog_account = db.Column(db.String, nullable=False)
    alert_channel_id = db.Column(db.Integer(big=True))
    backlogs_ingested = db.Column(db.Boolean())


class Webhooks(db.Table, table_name="webhooks"):
    server_id = db.Column(db.String, primary_key=True, unique=True, nullable=False)
    channel_id = db.Column(db.Integer(big=True), nullable=False)
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
            mod_role = self.bot.get_guild(result.server_id).get_role(result.role_id)
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
        """Add or update a subreddit. If you need help or have questions, contact Lil_SpazJoekp."""
        await context.defer()
        guild_id = context.guild.id
        if not context.channel:
            await context.send(
                embed=generate_result_embed(
                    "This must be used in a server.", result_type=EmbedType.Error, contact_me=True
                )
            )
            return
        subreddit = await SubredditConverter().convert(context, subreddit)
        mod_account = await RedditorConverter().convert(context, mod_account)
        if None in [subreddit, mod_account]:
            return
        results = parse_sql(
            await self.sql.fetch(
                "SELECT * FROM subreddits WHERE name=$1 AND server_id=$2",
                subreddit,
                guild_id,
            )
        )
        if results:
            confirm = await context.prompt(
                f"r/{subreddit} is already added. Do you want to overwrite it?", hidden=False
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
                    timeout=None,
                    hidden=False,
                )
                if not confirm:
                    await context.send("Cancelled")
        except credmgr.exceptions.NotFound:
            auth_url = self.bot.credmgr_bot.redditApp.genAuthUrl(required_scopes, True)
            confirm = await context.prompt(
                f"u/{mod_account} has not granted me permission yet, I will need you to authorize me using this link:\n{auth_url}.\n\nOnce you are done, please confirm below.\n\nIf you have any questions, please contact <@393801572858986496>.",
                timeout=None,
                hidden=False,
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
        else:
            await self.error_embed(context, f"u/{mod_account} does not moderate r/{subreddit}.")
            return
        if all(perm not in moderator.mod_permissions for perm in ["all", "posts"]):
            await self.error_embed(
                context,
                f"u/{mod_account} does not have enough permissions. Please ensure they have at least `posts` permissions and try again.\n\nIf you have any questions, please contact <@393801572858986496>.",
                contact_me=False,
            )
            return
        try:
            await self.sql.execute(
                "INSERT INTO subreddits (name, server_id, role_id, channel_id, modlog_account, alert_channel_id) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (name, server_id) DO UPDATE SET role_id=EXCLUDED.role_id, channel_id=EXCLUDED.channel_id, modlog_account=EXCLUDED.modlog_account, alert_channel_id=EXCLUDED.alert_channel_id",
                subreddit,
                guild_id,
                mod_role.id,
                channel.id,
                mod_account,
                alert_channel.id if alert_channel else alert_channel,
            )
            if alert_channel:
                await self.create_or_update_alert_channel(context, subreddit, alert_channel)
            self.restart_stream()
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
        if not context.channel:
            await context.send(
                embed=generate_result_embed(
                    "This must be used in a server.", result_type=EmbedType.Error, contact_me=True
                )
            )
            return
        subreddit = await SubredditConverter().convert(context, subreddit)
        if subreddit is None:
            return
        result = parse_sql(
            await self.sql.fetch(
                "SELECT * FROM subreddits WHERE name=$1 AND server_id=$2", subreddit, context.guild.id
            ),
            fetch_one=True,
        )
        if result:
            authorized_roles = await context.cog.get_bot_config("authorized_roles")
            is_authorized = any([role.id in authorized_roles for role in context.author.roles])
            if result.channel_id == context.channel_id or is_authorized:
                confirm = await context.prompt(
                    f"Are you *sure* you want to delete r/{subreddit} from this bot?", hidden=False
                )
                if not confirm:
                    return
                try:
                    await self.sql.execute(
                        "INSERT INTO deleted_subreddits (name, server_id, role_id, channel_id, modlog_account, alert_channel_id) VALUES ($1, $2, $3, $4, $5, $6)",
                        subreddit,
                        context.guild.id,
                        result.role_id,
                        result.channel_id,
                        result.modlog_account,
                        result.alert_channel_id,
                    )
                    await self.sql.execute(
                        "DELETE FROM webhooks WHERE subreddit=$1 AND server_id=$2", subreddit, context.guild.id
                    )
                    await self.sql.execute(
                        "DELETE FROM subreddits WHERE name=$1 AND server_id=$2", subreddit, context.guild.id
                    )
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
                    self.restart_stream()
                except Exception as error:
                    self.log.exception(error)
                    await self.error_embed(context, f"Failed to delete r/{subreddit}.")
            else:
                await self.error_embed(
                    context, f"You must execute this command from that subreddit's channel (<#{result.channel_id}>)."
                )
        else:
            await self.error_embed(context, f"r/{subreddit} has not been added to this bot.")

    @cog_subcommand(base="manage_sub", options=[create_option("subreddit", "Subreddit to view.", str, True)])
    async def view(self, context, subreddit):
        """View a subreddit."""
        await context.defer()
        if not context.channel:
            await context.send(
                embed=generate_result_embed(
                    "This must be used in server.", result_type=EmbedType.Error, contact_me=True
                )
            )
            return
        subreddit = await SubredditConverter().convert(context, subreddit)
        if subreddit is None:
            return
        result = parse_sql(
            await self.sql.fetch(
                "SELECT * FROM subreddits WHERE name=$1 AND server_id=$2", subreddit, context.guild.id
            ),
            fetch_one=True,
        )
        if result:
            embed = await self.generate_subreddit_embed(
                None, subreddit, result=result, title=f"r/{subreddit}", url=f"https://www.reddit.com/r/{subreddit}"
            )
            await context.send(embed=embed)
        else:
            await self.error_embed(context, f"r/{subreddit} has not been added to this bot.")

    async def create_or_update_alert_channel(self, context, subreddit, alert_channel):
        with open("admin.png", "rb") as file:
            admin_avatar = file.read()
        sub = await self.reddit.subreddit(subreddit, fetch=True)
        if sub.icon_img:
            try:
                response = await self.bot.session.get(sub.icon_img)
                mod_avatar = await response.read()
            except Exception:
                with open("mod.png", "rb") as file:
                    mod_avatar = file.read()
        else:
            with open("mod.png", "rb") as file:
                mod_avatar = file.read()
        mapping = {"admin_webhook": "Admin Action Alert", "alert_webhook": "Subreddit Alert"}
        webhook_names = ["admin_webhook", "alert_webhook"]
        result = parse_sql(
            await self.sql.fetch(
                "SELECT * FROM webhooks WHERE subreddit=$1 AND server_id=$2", subreddit, context.guild.id
            ),
            fetch_one=True,
        )
        webhooks = {}
        channel_webhooks = await alert_channel.webhooks()
        if result:
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
                    "UPDATE webhooks SET admin_webhook=$1, alert_webhook=$2 WHERE subreddit=$3 AND server_id=$4",
                    admin_webhook,
                    alert_webhook,
                    subreddit,
                    context.guild.id,
                )
        else:
            admin_webhook = await alert_channel.create_webhook(name=mapping["admin_webhook"], avatar=admin_avatar)
            alert_webhook = await alert_channel.create_webhook(name=mapping["alert_webhook"], avatar=mod_avatar)
            await self.sql.execute(
                "INSERT INTO webhooks (subreddit, server_id, admin_webhook, alert_webhook) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                subreddit,
                context.guild.id,
                admin_webhook.url,
                alert_webhook.url,
            )

    def restart_stream(self):
        if not self.bot.debug:
            try:
                os.system("pm2 restart RedditModHelper-Stream")
            except Exception as error:
                self.log.exception(error)

    async def verify_valid_auth(self, context, mod_account, required_scopes):
        final_failed_message = "Authorization failed. Please try again or contact <@393801572858986496>."
        try:
            reddit: praw.Reddit = self.bot.credmgr_bot.redditApp.reddit(mod_account)
            current_scopes = reddit.auth.scopes()
            if not set(required_scopes).issubset(current_scopes) and "*" not in current_scopes:
                await self.error_embed(context, final_failed_message, contact_me=False)
                return False
            else:
                return True
        except Exception:
            await self.error_embed(context, final_failed_message, contact_me=False)
            return False


def setup(bot):
    bot.add_cog(SubredditManagement(bot))
