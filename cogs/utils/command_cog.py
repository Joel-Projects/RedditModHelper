import asyncio
import json
import time
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
    async def error_embed(context, message):
        embed = Embed(
            title="Command Error", color=discord.Color.red(), description=message
        )
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
        return await context.send(embed=embed)

    async def warning_embed(self, context, message):
        embed = Embed(
            title="Warning", color=discord.Color.orange(), description=message
        )
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
        return await context.send(embed=embed)

    async def success_embed(
        self, context, message, title="Command Executed Successfully"
    ):
        if isinstance(message, Embed):
            embed = message
        else:
            embed = Embed(title=title, color=discord.Color.green(), description=message)
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
        return await context.send(embed=embed)

    async def status_embed(self, context, message, title="Status", *fields):
        embed = Embed(title=title, description=message, color=discord.Color.orange())
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
        for field in fields:
            embed.add_field(**field)
        msg = await context.send(embed=embed)
        return msg

    async def status_update_embed(self, msg, message, *fields):
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

    async def status_done_embed(self, msg, message, *fields):
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

    async def cancelledEmbed(self, context, message):
        embed = Embed(color=discord.Color.greyple())
        embed.title = f"Cancelled"
        embed.description = message
        embed.set_footer(
            text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime())
        )
        await context.send(embed=embed)

    async def get_bot_config(self, key):
        results = parse_sql(
            await self.bot.pool.fetch("SELECT * FROM settings WHERE key=$1", key)
        )
        if len(results) > 0:
            return json.loads(results[0].value)["value"]
        else:
            return None

    async def set_bot_config(self, **kwargs):
        update = []
        insert = []
        results = parse_sql(await self.bot.pool.fetch("SELECT key FROM settings"))
        existing = [
            i.key
            for i in results
        ] if results else []
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

    async def check_sub(self, context, subreddit):
        exists = True
        try:
            try:
                sub = await self.reddit.subreddit(subreddit, fetch=True)
                subreddit = sub.display_name
            except asyncprawcore.exceptions.Redirect as error:
                if error.path == "/subreddits/search":
                    exists = False
                    await self.error_embed(context, f"r/{subreddit} does not exist.")
            mods = await sub.moderator()
            me = await self.reddit.user.me()
            return {"exists": exists, "isMod": me in mods, "subreddit": subreddit}
        except Exception as error:
            if type(error) != asyncprawcore.exceptions.Redirect:
                self.log.exception(error)

    async def check_mod_sub(
        self, context, subreddit
    ) -> asyncpraw.reddit.models.Subreddit:
        exists = True
        try:
            try:
                sub = await self.reddit.subreddit(subreddit)
                subreddit = sub
            except asyncprawcore.exceptions.Redirect as error:
                if error.path == "/subreddits/search":
                    exists = False
                    await self.error_embed(
                        context, f"{subreddit.display_name} does not exist."
                    )
                    return None
            mods = await sub.moderator()
            me = await self.reddit.user.me()
            if me in mods:
                return subreddit
            else:
                await self.error_embed(
                    context, f"You don't moderate {subreddit.display_name}"
                )
                return None
        except Exception as error:
            if type(error) != asyncprawcore.exceptions.Redirect:
                self.log.exception(error)

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

    async def get_authorized_user(self, context):
        results = await self.sql.fetch(
            "SELECT modlog_account FROM subreddits WHERE channel_id=$1", context.channel.id
        )
        results = parse_sql(results)
        if results:
            return results[0][0]
        else:
            return None

    async def get_bot_config(self, key):
        results = await self.sql.fetch("SELECT * FROM settings WHERE key=$1", key)
        if len(results) > 0:
            return json.loads(results[0][1])["value"]
        else:
            return None

    async def get_moderated_subreddits(self, context, user=None):
        subreddits = []
        try:
            if not user:
                currentUser = await self.reddit.user.me()
                results = parse_sql(
                    await self.sql.fetch(
                        "SELECT redditor FROM modlogusers WHERE enabled"
                    )
                )
                fetchedSubreddits = []
                subredditsJson = []
                for result in results:
                    user = result.redditor
                    if "siouxsie_siouxv2" == currentUser:
                        jsonData = await self.reddit.get(
                            f"user/{user}/moderated_subreddits.json"
                        )
                        self.log.debug(jsonData)
                    else:
                        with context.bot.tempReddit(user) as reddit:
                            jsonData = await reddit.get(
                                f"user/{user}/moderated_subreddits.json"
                            )
                            self.log.debug(jsonData)
                    subredditsJson += jsonData["data"]
                return [
                    (i["sr"], i["subscribers"], fetchedSubreddits.append(i["sr"]))[:2]
                    for i in subredditsJson
                    if i["sr"] not in fetchedSubreddits
                ]
            else:
                subs = await self.reddit.get(f"user/{user}/moderated_subreddits.json")
                return subs
        except KeyError:
            await self.error_embed(
                context,
                f"[u/{user}](https://reddit.com/u/{user}) does not moderate any subreddits",
            )

    async def user_authed_check(self, context):
        bypassRole = discord.utils.get(context.guild.roles, id=594708941582368769)
        redditor = await self.get_user(context, context.author)
        if context.author in bypassRole.members:
            return redditor, True
        else:
            if redditor:
                tmpReddit = self.bot.services.reddit(redditor)
                user = None
                try:
                    user = tmpReddit.user.me()
                except:
                    pass
                if user:
                    return redditor, False
                else:
                    await self.error_embed(
                        context,
                        "You need to auth your reddit account before you can use this command.\nUse this link to auth: ",
                    )
                    return redditor, None
            else:
                await self.error_embed(
                    context,
                    "You need to auth your reddit account before you can use this command. Use this link to auth:",
                )
                return None, None

    async def get_user(self, context=None, member=None):
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
