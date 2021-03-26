import asyncpraw
import credmgr
import discord
import praw
from discord_slash.utils.manage_commands import create_option

from .utils import db
from .utils.command_cog import CommandCog
from .utils.commands import cog_slash
from .utils.converters import RedditorConverter, SubredditConverter
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
    async def addsub(
        self,
        context,
        subreddit: SubredditConverter,
        mod_role: discord.Role,
        channel: discord.TextChannel,
        mod_account: RedditorConverter,
        alert_channel: discord.TextChannel = None,
    ):
        """Adds a subreddit to the bot. If you need help or have questions, contact Lil_SpazJoekp."""
        await context.defer()
        if None in [subreddit, mod_account]:
            return
        results = parse_sql(await self.sql.fetch("SELECT * FROM subreddits WHERE name=$1", subreddit))
        if results:
            confirm = await context.prompt(
                f"r/{subreddit} is already added. Do you want to overwrite it?",
                delete_after=False,
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
                    delete_after=False,
                )
                if not confirm:
                    await context.send("Cancelled")
        except credmgr.exceptions.NotFound:
            auth_url = self.bot.credmgr_bot.redditApp.genAuthUrl(required_scopes, True)
            confirm = await context.prompt(
                f"u/{mod_account} has not granted me permission yet, I will need you to reauthorize me using this link:\n{auth_url}.\n\nOnce you are done, please confirm below.\n\nIf you have any questions, please contact <@393801572858986496>.",
                delete_after=False,
            )
            if not confirm:
                await context.send("Cancelled")
                await self.error_embed(context, f"Failed to add r/{subreddit}.")
        except Exception as error:
            self.log.exception(error)
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
            with open("redditadmin.png", "rb") as file:
                admin_avatar = file.read()
            with open("redditmod.png", "rb") as file:
                mod_avatar = file.read()
            sub = await self.reddit.subreddit(subreddit, fetch=True)
            if sub.icon_img:
                try:
                    response = await self.bot.session.get(sub.icon_img)
                    mod_avatar_temp = await response.read()
                    mod_avatar = mod_avatar_temp or mod_avatar
                except Exception:
                    pass
            mapping = {"admin_webhook": "Admin Action Alert", "alert_webhook": "Subreddit Alert"}
            webhook_names = ["admin_webhook", "alert_webhook"]
            results = parse_sql(await self.sql.fetch("SELECT * FROM webhooks WHERE subreddit=$1", subreddit))
            webhooks = {}
            channel_webhooks = await context.channel.webhooks()
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
