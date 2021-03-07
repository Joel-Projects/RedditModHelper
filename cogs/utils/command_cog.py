import asyncio
import json
import time
import typing
from typing import TYPE_CHECKING

import asyncpraw
import asyncprawcore
import credmgr
import discord
from asyncpg import Pool
from discord import Embed
from discord.ext import commands

from .utils import parse_sql

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
    async def cancelledEmbed(context, message):
        embed = Embed(color=discord.Color.greyple())
        embed.title = f"Cancelled"
        embed.description = message
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
        await context.send(embed=embed)

    @staticmethod
    async def error_embed(context, message):
        embed = Embed(
            title="Command Error", color=discord.Color.red(), description=message
        )
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
        return await context.send(embed=embed)

    @staticmethod
    async def status_done_embed(msg, message, *fields):
        if msg and msg.embeds:
            oldEmbed: Embed = msg.embeds[0]
            embed = Embed(
                title=oldEmbed.title, description=message, color=discord.Color.green()
            )
        else:
            embed = Embed(color=discord.Color.green(), description=message)
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
        embed.clear_fields()
        for field in fields:
            embed.add_field(**field)
        await msg.edit(embed=embed)
        return msg

    @staticmethod
    async def status_embed(context, message, title="Status", *fields):
        embed = Embed(title=title, description=message, color=discord.Color.orange())
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
        for field in fields:
            embed.add_field(**field)
        msg = await context.send(embed=embed)
        return msg

    @staticmethod
    async def status_update_embed(msg, message, *fields):
        if msg and msg.embeds:
            oldEmbed: Embed = msg.embeds[0]
            embed = Embed(
                title=oldEmbed.title, description=message, color=discord.Color.orange()
            )
        else:
            embed = Embed(color=discord.Color.orange(), description=message)
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
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
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
        return await context.send(embed=embed)

    @staticmethod
    async def warning_embed(context, message):
        embed = Embed(
            title="Warning", color=discord.Color.orange(), description=message
        )
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
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
                    await self.error_embed(
                        context, f"Only admins can specify {item_name}."
                    )
            elif items[0] not in allowed_items:
                await self.error_embed(
                    context, f"Only admins can specify other {item_name}."
                )
            return

    async def get_and_calculate_subs(self, user):
        moderated = None
        try:
            with self.bot.tempReddit(user) as reddit:
                moderated = await reddit.get(f"user/{user}/moderated_subreddits")
                if "data" not in moderated:
                    moderated = []
                else:
                    moderated = moderated["data"]
        except Exception:
            pass
        if not moderated:
            moderated = await self.bot.reddit.get(f"user/{user}/moderated_subreddits")
            if "data" not in moderated:
                moderated = []
            else:
                moderated = moderated["data"]
        subreddits = [(i["display_name"], i["subscribers"]) for i in moderated]
        subscribers = sum([subreddit[1] for subreddit in subreddits])
        subCount = len(subreddits)
        zeroCount = len([subreddit[1] for subreddit in subreddits if subreddit[1] == 0])
        remaining = len([subreddit[1] for subreddit in subreddits if subreddit[1] == 1])
        subAverage = int(round(subscribers / len(subreddits))) if subreddits else 0
        return remaining, subAverage, subCount, subreddits, subscribers, zeroCount

    async def get_authorized_user(self, context):
        results = await self.sql.fetch(
            "SELECT modlog_account FROM subreddits WHERE channel_id=$1",
            context.channel.id,
        )
        results = parse_sql(results)
        if results:
            return results[0][0]
        else:
            return None

    async def get_bot_config(self, key):
        results = parse_sql(
            await self.bot.pool.fetch("SELECT * FROM settings WHERE key=$1", key)
        )
        if len(results) > 0:
            return json.loads(results[0].value)["value"]
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
        results = await self.sql.fetch(
            "SELECT name FROM subreddits WHERE channel_id=$1", context.channel.id
        )
        results = parse_sql(results)
        if results:
            return results[0][0]
        else:
            return None

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
                            self.bot.loop.create_task(
                                self.error_embed(context, "Please choose one option")
                            )
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
            replyMessage = await self.bot.wait_for(
                "message", check=messageCheck, timeout=timeout
            )
            if any(
                [i in replyMessage.content.lower() for i in ["q", "quit", "s", "stop"]]
            ):
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
            await self.bot.pool.executemany(
                "UPDATE settings SET value=$2 WHERE key=$1", update
            )
        if insert:
            await self.bot.pool.executemany(
                "INSERT INTO settings (key, value) VALUES ($1, $2)", insert
            )
