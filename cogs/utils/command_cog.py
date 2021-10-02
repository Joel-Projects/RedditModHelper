import asyncio
import json
import time
import typing
from typing import TYPE_CHECKING

import asyncprawcore
import credmgr
import discord
from asyncpg import Pool
from discord import Embed
from discord.ext import commands

from .utils import parse_sql, readable_list

if TYPE_CHECKING:
    from ...bot import RedditModHelper


class CommandCog(commands.Cog):
    def __init__(self, bot):
        self.bot: "RedditModHelper" = bot
        self.log = bot.log
        self.reddit = bot.reddit
        self.sql: Pool = bot.sql
        self.tempReddit = None
        super().__init__()

    @staticmethod
    async def cancelled_embed(context, message):
        embed = Embed(color=discord.Color.greyple())
        embed.title = f"Cancelled"
        embed.description = message
        embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
        await context.send(embed=embed)

    @staticmethod
    async def error_embed(context, message, delete_after=None, contact_me=True):
        embed = Embed(
            title="Command Error",
            color=discord.Color.red(),
            description=f"{message}\n\nIf you need more help, contact <@393801572858986496>."
            if contact_me
            else message,
        )
        embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
        return await context.send(embed=embed, delete_after=delete_after)

    @staticmethod
    def generate_error_embed(message):
        embed = Embed(title="Command Error", color=discord.Color.red(), description=message)
        embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
        return embed

    @staticmethod
    async def status_done_embed(msg, message, *fields):
        if msg and msg.embeds:
            oldEmbed: Embed = msg.embeds[0]
            embed = Embed(title=oldEmbed.title, description=message, color=discord.Color.green())
        else:
            embed = Embed(color=discord.Color.green(), description=message)
        embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
        embed.clear_fields()
        for field in fields:
            embed.add_field(**field)
        await msg.edit(embed=embed)
        return msg

    @staticmethod
    async def status_embed(context, message, title="Status", *fields):
        embed = Embed(title=title, description=message, color=discord.Color.orange())
        embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
        for field in fields:
            embed.add_field(**field)
        msg = await context.send(embed=embed)
        return msg

    @staticmethod
    async def status_update_embed(msg, message, *fields):
        if msg and msg.embeds:
            oldEmbed: Embed = msg.embeds[0]
            embed = Embed(title=oldEmbed.title, description=message, color=discord.Color.orange())
        else:
            embed = Embed(color=discord.Color.orange(), description=message)
        embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
        embed.clear_fields()
        for field in fields:
            embed.add_field(**field)
        await msg.edit(embed=embed)
        return msg

    @staticmethod
    async def success_embed(context, message, title="Success!"):
        if isinstance(message, Embed):
            embed = message
        else:
            embed = Embed(title=title, color=discord.Color.green(), description=message)
        embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
        return await context.send(embed=embed)

    @staticmethod
    async def warning_embed(context, message):
        embed = Embed(title="Warning", color=discord.Color.orange(), description=message)
        embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
        return await context.send(embed=embed)

    async def check_multiple_auth(self, context, item_name, items, allowed_items=None):
        """Check if current user can specify multiple items,

        :param context:
        :param item_name:
        :param items:
        :param allowed_items:
        :return:
        """
        admin_roles = [
            discord.utils.get(context.guild.roles, id=role_id)
            for role_id in await self.get_bot_config("authorized_roles")
        ]
        authorized_users = [member for role in admin_roles for member in role.members]
        if allowed_items:
            if not isinstance(allowed_items, typing.Iterable):
                allowed_items = [allowed_items]
        if items:
            if len(items) > 1:
                if context.author in authorized_users:
                    return items
                else:
                    await self.error_embed(context, f"Only authorized users can specify {item_name}.")
            elif items[0] not in allowed_items:
                await self.error_embed(context, f"Only authorized users can specify other {item_name}.")
            return

    async def get_and_calculate_subs(self, user):
        moderated = None
        try:
            with self.bot.tempReddit(user) as reddit:
                redditor = await reddit.user.me()
                moderated = await redditor.moderated()
        except Exception as error:
            self.log.error(error)
            pass
        if not moderated:
            redditor = await reddit.redditor(user)
            moderated = await redditor.moderated()
        subreddits = [(i.display_name, i.subscribers) for i in moderated]
        subscribers = sum([subreddit[1] for subreddit in subreddits])
        sub_count = len(subreddits)
        zero_count = len([subreddit[1] for subreddit in subreddits if subreddit[1] == 0])
        remaining = len([subreddit[1] for subreddit in subreddits if subreddit[1] == 1])
        sub_average = int(round(subscribers / len(subreddits))) if subreddits else 0
        return remaining, sub_average, sub_count, subreddits, subscribers, zero_count

    async def get_authorized_user(self, context):
        if self.bot.debug and context.channel.id == 816020436940226611:
            return "Lil_SpazJoekp"
        result = parse_sql(
            await self.sql.fetch(
                "SELECT modlog_account FROM subreddits WHERE channel_id=$1",
                context.channel.id,
            ),
            fetch_one=True,
        )
        if result:
            return result[0]
        else:
            return None

    async def get_subreddit_instance(self, context, required_permissions=None):
        subreddit = await self.get_sub_from_channel(context)
        if not subreddit:
            return
        account = await self.get_authorized_user(context)
        if required_permissions:
            if isinstance(required_permissions, str):
                required_permissions = [required_permissions]
        else:
            required_permissions = []
        if not account:
            await self.error_embed(
                context,
                "This command requires a mod account set for the subreddit.",
            )
            return
        try:
            if account == "Lil_SpazJoekp":
                reddit = self.reddit
            else:
                reddit = self.bot.get_reddit(account)
            subreddits = await (await reddit.user.me()).moderated()
            if subreddit in subreddits:
                sub = subreddits[subreddits.index(subreddit)]
                mod_info = (await sub.moderator(account))[0]
                has_all = "all" in mod_info.mod_permissions
                if not has_all and not all(
                    [
                        required_permission in mod_info.mod_permissions
                        for required_permission in required_permissions or mod_info.mod_permissions
                    ]
                ):
                    await self.error_embed(
                        context,
                        f"The moderator account set for this subreddit does not have the adequate permissions to run this command. This command requires the {readable_list(required_permissions, True)} permission{'s' if len(required_permissions) > 1 else ''}.",
                    )
                    return
            else:
                await self.error_embed(context, "The moderator account set for this subreddit is not a moderator.")
                return
        except Exception:
            await self.error_embed(
                context, "The authorization for the moderator account set for this subreddit is not valid."
            )
            return
        return sub

    async def get_bot_config(self, key):
        result = parse_sql(await self.bot.pool.fetch("SELECT * FROM settings WHERE key=$1", key), fetch_one=True)
        if result:
            return json.loads(result.value)["value"]
        else:
            return None

    async def get_mod(self, context, mod: str, returnAttr="name"):
        try:
            redditor = await self.reddit.redditor(mod, fetch=True)
        except asyncprawcore.NotFound:
            await self.error_embed(context, f"Could not find u/{mod}")
            return None
        else:
            if returnAttr:
                return getattr(redditor, returnAttr, None)
            else:
                return redditor

    async def get_sub(self, context, sub: str):
        try:
            sub = await self.reddit.subreddit(sub, fetch=True)
            return sub.display_name
        except asyncprawcore.exceptions.Redirect as error:
            if error.path == "/subreddits/search":
                await self.error_embed(context, f"r/{sub} does not exist.")

    async def get_sub_from_channel(self, context):
        if self.bot.debug and context.channel.id == 816020436940226611:
            return "pics"
        result = parse_sql(
            await self.sql.fetch("SELECT name FROM subreddits WHERE channel_id=$1", context.channel.id), fetch_one=True
        )
        if result:
            return result[0]
        else:
            await self.error_embed(context, "This command can only be used in a sub channel.")
            return

    async def get_redditor(self, context=None, member=None):
        if not member:
            if context:
                member = context.author
        user = None
        if isinstance(member, (discord.Member, discord.User)):
            member_id = member.id
        elif isinstance(member, int):
            member_id = member
        else:
            member_id = None
        if member_id:
            try:
                verification = self.bot.credmgr.userVerification(str(member_id))
                if verification:
                    user = verification.redditor
            except credmgr.exceptions.NotFound:
                pass
        return user

    async def prompt_options(
        self,
        context,
        title,
        description,
        options,
        objectName,
        timeout=120,
        multiSelect=False,
    ):
        embed: Embed = Embed(title=title, description=description)
        if objectName == "fields":
            for field in options:
                embed.add_field(name=field.name, value=field.value)
        else:
            embed.add_field(name=objectName, value="\n".join(options))
        self.toDelete = []
        self.toDelete.append(await context.send(embed=embed))

        def messageCheck(message):
            content = message.content
            if context.channel == message.channel and context.author == message.author:
                if any([i in content.lower() for i in ["q", "quit", "s", "stop"]]):
                    return True
                if multiSelect:
                    selectedOptions = content.split(" ")
                    for option in selectedOptions:
                        if option.isdigit():
                            _ = int(option)
                        else:
                            self.toDelete.append(
                                self.bot.loop.create_task(
                                    self.error_embed(
                                        context,
                                        f"{option} isn't a number or is invalid.",
                                    )
                                )
                            )
                            return False
                else:
                    if " " in content:
                        self.toDelete.append(
                            self.bot.loop.create_task(self.error_embed(context, "Please choose one option"))
                        )
                    else:
                        if content.isdigit():
                            _ = int(content)
                        else:
                            self.toDelete.append(
                                self.bot.loop.create_task(
                                    self.error_embed(
                                        context,
                                        f"{content} isn't a number or is invalid.",
                                    )
                                )
                            )
                            return False

                return True
            else:
                return False

        try:
            replyMessage = await self.bot.wait_for("message", check=messageCheck, timeout=timeout)
            if any([i in replyMessage.content.lower() for i in ["q", "quit", "s", "stop"]]):
                return
            content = replyMessage.content
        except asyncio.TimeoutError:
            self.toDelete.append(await context.channel.send("Took too long."))
            await asyncio.sleep(5)
        else:
            if multiSelect:
                return [int(i) - 1 for i in content.split(" ")]
            else:
                return int(content)
        try:
            await context.channel.delete_messages(self.toDelete)
        except Exception:
            pass

    async def set_bot_config(self, **kwargs):
        update = []
        insert = []
        results = parse_sql(await self.bot.pool.fetch("SELECT key FROM settings"))
        existing = [i.key for i in results] if results else []
        for key, value in kwargs.items():
            data = (key, json.dumps({"value": value}))
            if key in existing:
                update.append(data)
            else:
                insert.append(data)
        if update:
            await self.bot.pool.executemany("UPDATE settings SET value=$2 WHERE key=$1", update)
        if insert:
            await self.bot.pool.executemany("INSERT INTO settings (key, value) VALUES ($1, $2)", insert)
