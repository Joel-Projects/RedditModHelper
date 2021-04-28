import asyncio
import datetime
import gc
import io
import logging
import os
import re
import textwrap
import time as ttime
import traceback
import typing
from collections import Counter, defaultdict

import asyncpg
import discord
import pkg_resources
import psutil
from discord.ext import commands, menus, tasks

from .utils import context, db, formats, time
from .utils.command_cog import CommandCog
from .utils.commands import command, group
from .utils.context import Context

log = logging.getLogger(__name__)

LOGGING_CHANNEL = 817835810593439784


class GatewayHandler(logging.Handler):
    def __init__(self, cog):
        self.cog = cog
        super().__init__(logging.INFO)

    def filter(self, record):
        if isinstance(record.msg, str):
            return record.name == "discord.gateway" or "Shard ID" in record.msg or "Websocket closed " in record.msg
        else:
            return False

    def emit(self, record):
        self.cog.add_record(record)


class Commands(db.Table):
    id = db.PrimaryKeyColumn()

    guild_id = db.Column(db.Integer(big=True), index=True)
    channel_id = db.Column(db.Integer(big=True))
    author_id = db.Column(db.Integer(big=True), index=True)
    used = db.Column(db.Datetime, index=True)
    prefix = db.Column(db.String)
    command = db.Column(db.String, index=True)
    failed = db.Column(db.Boolean, index=True)


_INVITE_REGEX = re.compile(r"(?:https?:\/\/)?discord(?:\.gg|\.com|app\.com\/invite)?\/[A-Za-z0-9]+")


def censor_invite(obj, *, _regex=_INVITE_REGEX):
    return _regex.sub("[censored-invite]", str(obj))


def hex_value(arg):
    return int(arg, base=16)


def object_at(addr):
    for o in gc.get_objects():
        if id(o) == addr:
            return o
    return None


class Stats(CommandCog):
    """Bot usage statistics."""

    def __init__(self, bot):
        super().__init__(bot)
        self.process = psutil.Process()
        self._batch_lock = asyncio.Lock(loop=bot.loop)
        self._data_batch = []
        self.bulk_insert_loop.add_exception_type(asyncpg.PostgresConnectionError)
        self.bulk_insert_loop.start()
        self._gateway_queue = asyncio.Queue(loop=bot.loop)
        self.gateway_worker.start()

    async def bulk_insert(self):
        query = """INSERT INTO commands (guild_id, channel_id, author_id, used, prefix, command, failed)
                   SELECT x.guild, x.channel, x.author, x.used, x.prefix, x.command, x.failed
                   FROM jsonb_to_recordset($1::jsonb) AS
                   x(guild BIGINT, channel BIGINT, author BIGINT, used TIMESTAMP, prefix TEXT, command TEXT, failed BOOLEAN)
                """

        if self._data_batch:
            await self.bot.pool.execute(query, self._data_batch)
            total = len(self._data_batch)
            if total > 1:
                self.log.info(f"Registered {total} commands to the database.")
            self._data_batch.clear()

    def cog_unload(self):
        self.bulk_insert_loop.stop()
        self.gateway_worker.cancel()

    @tasks.loop(seconds=10.0)
    async def bulk_insert_loop(self):
        async with self._batch_lock:
            await self.bulk_insert()

    @tasks.loop(seconds=0.0)
    async def gateway_worker(self):
        record = await self._gateway_queue.get()
        await self.notify_gateway_status(record)

    async def register_command(self, context):
        if context.command is None:
            return

        command = context.command.qualified_name
        self.bot.command_stats[command] += 1
        message = context.message
        destination = None
        if context.guild is None:
            destination = "Private Message"
            guild_id = None
        else:
            destination = f"#{message.channel} ({message.guild})"
            guild_id = context.guild.id

        log.info(f"{message.created_at}: {message.author} in {destination}: {message.content}")
        async with self._batch_lock:
            self._data_batch.append(
                {
                    "guild": guild_id,
                    "channel": context.channel.id,
                    "author": context.author.id,
                    "used": message.created_at.isoformat(),
                    "prefix": context.prefix,
                    "command": command,
                    "failed": context.command_failed,
                }
            )

    @commands.Cog.listener()
    async def on_command_completion(self, context):
        await self.register_command(context)

    @commands.Cog.listener()
    async def on_socket_response(self, msg):
        self.bot.socket_stats[msg.get("t")] += 1

    @discord.utils.cached_property
    def webhook(self):
        wh_id, wh_token = self.bot.config.stat_webhook
        hook = discord.Webhook.partial(
            id=wh_id,
            token=wh_token,
            adapter=discord.AsyncWebhookAdapter(self.bot.session),
        )
        return hook

    async def log_error(self, *, context=None, extra=None):
        embed = discord.Embed(title="Error", color=0xDD5F53)
        embed.description = f"```py\n{traceback.format_exc()}\n```"
        embed.add_field(name="Extra", value=extra, inline=False)
        embed.timestamp = datetime.datetime.utcnow()

        if context is not None:
            fmt = "{0} (ID: {0.id})"
            author = fmt.format(context.author)
            channel = fmt.format(context.channel)
            guild = "None" if context.guild is None else fmt.format(context.guild)

            embed.add_field(name="Author", value=author)
            embed.add_field(name="Channel", value=channel)
            embed.add_field(name="Guild", value=guild)

        await self.webhook.send(embed=embed)

    @command(hidden=True)
    @commands.is_owner()
    async def commandstats(self, context, limit=20):
        """Shows command stats.

        Use a negative number for bottom instead of top.
        This is only for the current session.
        """
        counter = self.bot.command_stats
        width = len(max(counter, key=len))
        total = sum(counter.values())

        if limit > 0:
            common = counter.most_common(limit)
        else:
            common = counter.most_common()[limit:]

        output = "\n".join(f"{k:<{width}}: {c}" for k, c in common)

        await context.send(f"```\n{output}\n```")

    @command(hidden=True)
    async def socketstats(self, context):
        delta = datetime.datetime.utcnow() - self.bot.uptime
        minutes = delta.total_seconds() / 60
        total = sum(self.bot.socket_stats.values())
        cpm = total / minutes
        await context.send(f"{total} socket events observed ({cpm:.2f}/minute):\n{self.bot.socket_stats}")

    def get_bot_uptime(self, *, brief=False):
        return time.human_timedelta(self.bot.uptime, accuracy=None, brief=brief, suffix=False)

    @command()
    async def uptime(self, context):
        """Tells you how long the bot has been up for."""
        await context.send(f"Uptime: **{self.get_bot_uptime()}**")

    @command()
    async def about(self, context):
        """Tells you information about the bot itself."""

        embed = discord.Embed()
        embed.title = f"{self.bot.config.bot_name} Stats"
        embed.color = discord.Color.blurple()

        owner = self.bot.get_user(self.bot.owner_id)
        embed.set_author(name=str(owner), icon_url=owner.avatar_url)

        # statistics
        total_members = 0
        total_unique = len(self.bot.users)

        text = 0
        voice = 0
        guilds = 0
        for guild in self.bot.guilds:
            guilds += 1
            total_members += guild.member_count
            for channel in guild.channels:
                if isinstance(channel, discord.TextChannel):
                    text += 1
                elif isinstance(channel, discord.VoiceChannel):
                    voice += 1

        embed.add_field(name="Members", value=f"{total_members} total\n{total_unique} unique")
        embed.add_field(name="Channels", value=f"{text + voice} total\n{text} text\n{voice} voice")

        memory_usage = self.process.memory_full_info().uss / 1024 ** 2
        cpu_usage = self.process.cpu_percent() / psutil.cpu_count()
        embed.add_field(name="Process", value=f"{memory_usage:.2f} MiB\n{cpu_usage:.2f}% CPU")

        version = pkg_resources.get_distribution("discord.py").version
        embed.add_field(name="Guilds", value=guilds)
        embed.add_field(name="Commands Run", value=sum(self.bot.command_stats.values()))
        embed.add_field(name="Uptime", value=self.get_bot_uptime(brief=True))
        embed.set_footer(
            text=f"Made with discord.py v{version}",
            icon_url="https://i.imgur.com/5BFecvA.png",
        )
        embed.timestamp = datetime.datetime.now().astimezone()
        await context.send(embed=embed)

    def censor_object(self, obj):
        if not isinstance(obj, str) and obj.id in self.bot.blacklist:
            return "[censored]"
        return censor_invite(obj)

    async def show_guild_stats(self, context):
        lookup = (
            "\N{FIRST PLACE MEDAL}",
            "\N{SECOND PLACE MEDAL}",
            "\N{THIRD PLACE MEDAL}",
            "\N{SPORTS MEDAL}",
            "\N{SPORTS MEDAL}",
        )

        embed = discord.Embed(title="Server Command Stats", color=discord.Color.blurple())

        # total command uses
        query = "SELECT COUNT(*), MIN(used) FROM commands WHERE guild_id=$1;"
        count = await context.db.fetchrow(query, context.guild.id)

        embed.description = f"{count[0]} commands used."
        embed.set_footer(text="Tracking command usage since").timestamp = count[1] or datetime.datetime.utcnow()

        query = """SELECT command,
                          COUNT(*) as "uses"
                   FROM commands
                   WHERE guild_id=$1
                   GROUP BY command
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query, context.guild.id)

        value = (
            "\n".join(f"{lookup[index]}: {command} ({uses} uses)" for (index, (command, uses)) in enumerate(records))
            or "No Commands"
        )

        embed.add_field(name="Top Commands", value=value, inline=True)

        query = """SELECT command,
                          COUNT(*) as "uses"
                   FROM commands
                   WHERE guild_id=$1
                   AND used > (CURRENT_TIMESTAMP - INTERVAL '1 day')
                   GROUP BY command
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query, context.guild.id)

        value = (
            "\n".join(f"{lookup[index]}: {command} ({uses} uses)" for (index, (command, uses)) in enumerate(records))
            or "No Commands."
        )
        embed.add_field(name="Top Commands Today", value=value, inline=True)
        embed.add_field(name="\u200b", value="\u200b", inline=True)

        query = """SELECT author_id,
                          COUNT(*) AS "uses"
                   FROM commands
                   WHERE guild_id=$1
                   GROUP BY author_id
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query, context.guild.id)

        value = (
            "\n".join(
                f"{lookup[index]}: <@!{author_id}> ({uses} bot uses)"
                for (index, (author_id, uses)) in enumerate(records)
            )
            or "No bot users."
        )

        embed.add_field(name="Top Command Users", value=value, inline=True)

        query = """SELECT author_id,
                          COUNT(*) AS "uses"
                   FROM commands
                   WHERE guild_id=$1
                   AND used > (CURRENT_TIMESTAMP - INTERVAL '1 day')
                   GROUP BY author_id
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query, context.guild.id)

        value = (
            "\n".join(
                f"{lookup[index]}: <@!{author_id}> ({uses} bot uses)"
                for (index, (author_id, uses)) in enumerate(records)
            )
            or "No command users."
        )

        embed.add_field(name="Top Command Users Today", value=value, inline=True)
        await context.send(embed=embed)

    async def show_member_stats(self, context, member):
        lookup = (
            "\N{FIRST PLACE MEDAL}",
            "\N{SECOND PLACE MEDAL}",
            "\N{THIRD PLACE MEDAL}",
            "\N{SPORTS MEDAL}",
            "\N{SPORTS MEDAL}",
        )

        embed = discord.Embed(title="Command Stats", color=member.color)
        embed.set_author(name=str(member), icon_url=member.avatar_url)

        # total command uses
        query = "SELECT COUNT(*), MIN(used) FROM commands WHERE guild_id=$1 AND author_id=$2;"
        count = await context.db.fetchrow(query, context.guild.id, member.id)

        embed.description = f"{count[0]} commands used."
        embed.set_footer(text="First command used").timestamp = count[1] or datetime.datetime.utcnow()

        query = """SELECT command,
                          COUNT(*) as "uses"
                   FROM commands
                   WHERE guild_id=$1 AND author_id=$2
                   GROUP BY command
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query, context.guild.id, member.id)

        value = (
            "\n".join(f"{lookup[index]}: {command} ({uses} uses)" for (index, (command, uses)) in enumerate(records))
            or "No Commands"
        )

        embed.add_field(name="Most Used Commands", value=value, inline=False)

        query = """SELECT command,
                          COUNT(*) as "uses"
                   FROM commands
                   WHERE guild_id=$1
                   AND author_id=$2
                   AND used > (CURRENT_TIMESTAMP - INTERVAL '1 day')
                   GROUP BY command
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query, context.guild.id, member.id)

        value = (
            "\n".join(f"{lookup[index]}: {command} ({uses} uses)" for (index, (command, uses)) in enumerate(records))
            or "No Commands"
        )

        embed.add_field(name="Most Used Commands Today", value=value, inline=False)
        await context.send(embed=embed)

    @group(invoke_without_command=True)
    @commands.guild_only()
    @commands.cooldown(1, 30.0, type=commands.BucketType.member)
    async def stats(self, context, *, member: discord.Member = None):
        """Tells you command usage stats for the server or a member."""
        async with context.typing():
            if member is None:
                await self.show_guild_stats(context)
            else:
                await self.show_member_stats(context, member)

    @stats.command(name="global")
    @commands.is_owner()
    async def stats_global(self, context):
        """Global all time command statistics."""

        query = "SELECT COUNT(*) FROM commands;"
        total = await context.db.fetchrow(query)

        embed = discord.Embed(title="Command Stats", color=discord.Color.blurple())
        embed.description = f"{total[0]} commands used."

        lookup = (
            "\N{FIRST PLACE MEDAL}",
            "\N{SECOND PLACE MEDAL}",
            "\N{THIRD PLACE MEDAL}",
            "\N{SPORTS MEDAL}",
            "\N{SPORTS MEDAL}",
        )

        query = """SELECT command, COUNT(*) AS "uses"
                   FROM commands
                   GROUP BY command
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query)
        value = "\n".join(
            f"{lookup[index]}: {command} ({uses} uses)" for (index, (command, uses)) in enumerate(records)
        )
        embed.add_field(name="Top Commands", value=value, inline=False)

        query = """SELECT guild_id, COUNT(*) AS "uses"
                   FROM commands
                   GROUP BY guild_id
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query)
        value = []
        for (index, (guild_id, uses)) in enumerate(records):
            if guild_id is None:
                guild = "Private Message"
            else:
                guild = self.censor_object(self.bot.get_guild(guild_id) or f"<Unknown {guild_id}>")

            emoji = lookup[index]
            value.append(f"{emoji}: {guild} ({uses} uses)")

        embed.add_field(name="Top Guilds", value="\n".join(value), inline=False)

        query = """SELECT author_id, COUNT(*) AS "uses"
                   FROM commands
                   GROUP BY author_id
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query)
        value = []
        for (index, (author_id, uses)) in enumerate(records):
            user = self.censor_object(self.bot.get_user(author_id) or f"<Unknown {author_id}>")
            emoji = lookup[index]
            value.append(f"{emoji}: {user} ({uses} uses)")

        embed.add_field(name="Top Users", value="\n".join(value), inline=False)
        await context.send(embed=embed)

    @stats.command(name="today")
    @commands.is_owner()
    async def stats_today(self, context):
        """Global command statistics for the day."""

        query = (
            "SELECT failed, COUNT(*) FROM commands WHERE used > (CURRENT_TIMESTAMP - INTERVAL '1 day') GROUP BY failed;"
        )
        total = await context.db.fetch(query)
        failed = 0
        success = 0
        question = 0
        for state, count in total:
            if state is False:
                success += count
            elif state is True:
                failed += count
            else:
                question += count

        embed = discord.Embed(title="Last 24 Hour Command Stats", color=discord.Color.blurple())
        embed.description = f"{failed + success + question} commands used today. ({success} succeeded, {failed} failed, {question} unknown)"

        lookup = (
            "\N{FIRST PLACE MEDAL}",
            "\N{SECOND PLACE MEDAL}",
            "\N{THIRD PLACE MEDAL}",
            "\N{SPORTS MEDAL}",
            "\N{SPORTS MEDAL}",
        )

        query = """SELECT command, COUNT(*) AS "uses"
                   FROM commands
                   WHERE used > (CURRENT_TIMESTAMP - INTERVAL '1 day')
                   GROUP BY command
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query)
        value = "\n".join(
            f"{lookup[index]}: {command} ({uses} uses)" for (index, (command, uses)) in enumerate(records)
        )
        embed.add_field(name="Top Commands", value=value, inline=False)

        query = """SELECT guild_id, COUNT(*) AS "uses"
                   FROM commands
                   WHERE used > (CURRENT_TIMESTAMP - INTERVAL '1 day')
                   GROUP BY guild_id
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query)
        value = []
        for (index, (guild_id, uses)) in enumerate(records):
            if guild_id is None:
                guild = "Private Message"
            else:
                guild = self.censor_object(self.bot.get_guild(guild_id) or f"<Unknown {guild_id}>")
            emoji = lookup[index]
            value.append(f"{emoji}: {guild} ({uses} uses)")

        embed.add_field(name="Top Guilds", value="\n".join(value), inline=False)

        query = """SELECT author_id, COUNT(*) AS "uses"
                   FROM commands
                   WHERE used > (CURRENT_TIMESTAMP - INTERVAL '1 day')
                   GROUP BY author_id
                   ORDER BY "uses" DESC
                   LIMIT 5;
                """

        records = await context.db.fetch(query)
        value = []
        for (index, (author_id, uses)) in enumerate(records):
            user = self.censor_object(self.bot.get_user(author_id) or f"<Unknown {author_id}>")
            emoji = lookup[index]
            value.append(f"{emoji}: {user} ({uses} uses)")

        embed.add_field(name="Top Users", value="\n".join(value), inline=False)
        await context.send(embed=embed)

    async def send_guild_stats(self, embed, guild):
        embed.add_field(name="Name", value=guild.name)
        embed.add_field(name="ID", value=guild.id)
        embed.add_field(name="Shard ID", value=guild.shard_id or "N/A")
        embed.add_field(name="Owner", value=f"{guild.owner} (ID: {guild.owner_id})")

        bots = sum(m.bot for m in guild.members)
        total = guild.member_count
        online = sum(m.status is discord.Status.online for m in guild.members)
        embed.add_field(name="Members", value=str(total))
        embed.add_field(name="Members", value=str(total))
        embed.add_field(name="Bots", value=f"{bots} ({bots/total:.2%})")

        if guild.icon:
            embed.set_thumbnail(url=guild.icon_url)

        if guild.me:
            embed.timestamp = guild.me.joined_at

        await self.webhook.send(embed=embed)

    @stats_today.before_invoke
    @stats_global.before_invoke
    async def before_stats_invoke(self, context):
        await context.trigger_typing()

    @commands.Cog.listener()
    async def on_guild_join(self, guild):
        embed = discord.Embed(color=0x53DDA4, title="New Guild")  # green color
        await self.send_guild_stats(embed, guild)

    @commands.Cog.listener()
    async def on_guild_remove(self, guild):
        embed = discord.Embed(color=0xDD5F53, title="Left Guild")  # red color
        await self.send_guild_stats(embed, guild)

    @commands.Cog.listener()
    async def on_command_error(self, context, error):
        await self.register_command(context)
        if not isinstance(error, (commands.CommandInvokeError, commands.ConversionError)):
            return

        error = error.original
        if isinstance(error, (discord.Forbidden, discord.NotFound, menus.MenuError)):
            return

        embed = discord.Embed(title="Command Error", color=0xCC3366)
        embed.add_field(name="Name", value=context.command.qualified_name)
        embed.add_field(name="Author", value=f"{context.author} (ID: {context.author.id})")

        fmt = f"Channel: {context.channel} (ID: {context.channel.id})"
        if context.guild:
            fmt = (
                f"{fmt}\nGuild: {context.guild} (ID: {context.guild.id})\n[Invoke Message]({context.message.jump_url})"
            )
        embed.add_field(name="Location", value=fmt, inline=False)
        embed.add_field(name="Content", value=textwrap.shorten(context.message.content, width=512))

        exc = "".join(traceback.format_exception(type(error), error, error.__traceback__, chain=False))
        embed.description = f"```py\n{exc}\n```"
        embed.timestamp = datetime.datetime.utcnow()
        await self.webhook.send(embed=embed)

    def add_record(self, record):
        self._gateway_queue.put_nowait(record)

    async def notify_gateway_status(self, record):
        attributes = {"INFO": "\N{INFORMATION SOURCE}", "WARNING": "\N{WARNING SIGN}"}

        emoji = attributes.get(record.levelname, "\N{CROSS MARK}")
        dt = datetime.datetime.utcfromtimestamp(record.created)
        msg = textwrap.shorten(f"{emoji} `[{dt:%m-%d-%Y %I:%M:%S %p}] {record.message}`", width=1990)
        await self.webhook.send(msg, username="Gateway", avatar_url="https://i.imgur.com/4PnCKB3.png")

    @command(hidden=True)
    @commands.is_owner()
    async def bothealth(self, context):
        """Various bot health monitoring tools."""

        # This uses a lot of private methods because there is no
        # clean way of doing this otherwise.

        HEALTHY = discord.Color(value=0x43B581)
        UNHEALTHY = discord.Color(value=0xF04947)
        WARNING = discord.Color(value=0xF09E47)
        total_warnings = 0

        embed = discord.Embed(title="Bot Health Report", color=HEALTHY)

        # Check the connection pool health.
        pool = self.bot.pool
        total_waiting = len(pool._queue._getters)
        current_generation = pool._generation

        description = [
            f"Total `Pool.acquire` Waiters: {total_waiting}",
            f"Current Pool Generation: {current_generation}",
            f"Connections In Use: {len(pool._holders) - pool._queue.qsize()}",
        ]

        questionable_connections = 0
        connection_value = []
        for index, holder in enumerate(pool._holders, start=1):
            generation = holder._generation
            in_use = holder._in_use is not None
            is_closed = holder._con is None or holder._con.is_closed()
            display = f"gen={holder._generation} in_use={in_use} closed={is_closed}"
            questionable_connections += any((in_use, generation != current_generation))
            connection_value.append(f"<Holder i={index} {display}>")

        joined_value = "\n".join(connection_value)
        embed.add_field(name="Connections", value=f"```py\n{joined_value}\n```", inline=False)

        description.append(f"Questionable Connections: {questionable_connections}")

        total_warnings += questionable_connections

        try:
            task_retriever = asyncio.Task.all_tasks
        except AttributeError:
            # future proofing for 3.9 I guess
            task_retriever = asyncio.all_tasks
        else:
            all_tasks = task_retriever(loop=self.bot.loop)

        event_tasks = [t for t in all_tasks if "Client._run_event" in repr(t) and not t.done()]

        cogs_directory = os.path.dirname(__file__)
        tasks_directory = os.path.join("discord", "ext", "tasks", "__init__.py")
        inner_tasks = [t for t in all_tasks if cogs_directory in repr(t) or tasks_directory in repr(t)]

        bad_inner_tasks = ", ".join(hex(id(t)) for t in inner_tasks if t.done() and t._exception is not None)
        total_warnings += bool(bad_inner_tasks)
        embed.add_field(
            name="Inner Tasks",
            value=f'Total: {len(inner_tasks)}\nFailed: {bad_inner_tasks or "None"}',
        )
        embed.add_field(name="Events Waiting", value=f"Total: {len(event_tasks)}", inline=False)

        command_waiters = len(self._data_batch)
        is_locked = self._batch_lock.locked()
        description.append(f"Commands Waiting: {command_waiters}, Batch Locked: {is_locked}")

        memory_usage = self.process.memory_full_info().uss / 1024 ** 2
        cpu_usage = self.process.cpu_percent() / psutil.cpu_count()
        embed.add_field(
            name="Process",
            value=f"{memory_usage:.2f} MiB\n{cpu_usage:.2f}% CPU",
            inline=False,
        )

        global_rate_limit = not self.bot.http._global_over.is_set()
        description.append(f"Global Rate Limit: {global_rate_limit}")

        if command_waiters >= 8:
            total_warnings += 1
            embed.color = WARNING

        if global_rate_limit or total_warnings >= 9:
            embed.color = UNHEALTHY

        embed.set_footer(text=f"{total_warnings} warning(s)")
        embed.description = "\n".join(description)
        await context.send(embed=embed)

    @command(hidden=True)
    @commands.is_owner()
    async def gateway(self, context):
        """Gateway related stats."""

        yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        identifies = {
            shard_id: sum(1 for dt in dates if dt > yesterday) for shard_id, dates in self.bot.identifies.items()
        }
        resumes = {shard_id: sum(1 for dt in dates if dt > yesterday) for shard_id, dates in self.bot.resumes.items()}

        total_identifies = sum(identifies.values())

        builder = [
            f"Total RESUMEs: {sum(resumes.values())}",
            f"Total IDENTIFYs: {total_identifies}",
        ]

        shard_count = len(self.bot.shards)
        if total_identifies > (shard_count * 10):
            issues = 2 + (total_identifies // 10) - shard_count
        else:
            issues = 0

        for shard_id, shard in self.bot.shards.items():
            badge = None
            # Shard WS closed
            # Shard Task failure
            # Shard Task complete (no failure)
            if shard.is_closed():
                badge = "<:offline:646449055047876628>"
                issues += 1
            elif shard._parent._task.done():
                exc = shard._parent._task.exception()
                if exc is not None:
                    badge = "\N{FIRE}"
                    issues += 1
                else:
                    badge = "\U0001f504"

            if badge is None:
                badge = "<:online:646449055014322176>"

            stats = []
            identify = identifies.get(shard_id, 0)
            resume = resumes.get(shard_id, 0)
            if resume != 0:
                stats.append(f"R: {resume}")
            if identify != 0:
                stats.append(f"ID: {identify}")

            if stats:
                builder.append(f'Shard ID {shard_id}: {badge} ({", ".join(stats)})')
            else:
                builder.append(f"Shard ID {shard_id}: {badge}")

        if issues == 0:
            color = 0x43B581
        elif issues < len(self.bot.shards) // 4:
            color = 0xF09E47
        else:
            color = 0xF04947

        embed = discord.Embed(color=color, title="Gateway (last 24 hours)")
        embed.description = "\n".join(builder)
        embed.set_footer(text=f"{issues} warnings")
        await context.send(embed=embed)

    @command(hidden=True, aliases=["cancel_task"])
    @commands.is_owner()
    async def debug_task(self, context, memory_id: hex_value):
        """Debug a task by a memory location."""
        task = object_at(memory_id)
        if task is None or not isinstance(task, asyncio.Task):
            return await context.send(f"Could not find Task object at {hex(memory_id)}.")

        if context.invoked_with == "cancel_task":
            task.cancel()
            return await context.send(f"Cancelled task object {task!r}.")

        paginator = commands.Paginator(prefix="```py")
        fp = io.StringIO()
        frames = len(task.get_stack())
        paginator.add_line(f"# Total Frames: {frames}")
        task.print_stack(file=fp)

        for line in fp.getvalue().splitlines():
            paginator.add_line(line)

        for page in paginator.pages:
            await context.send(page)

    async def tabulate_query(self, context, query, *args):
        records = await context.db.fetch(query, *args)

        if len(records) == 0:
            return await context.send("No results found.")

        headers = list(records[0].keys())
        table = formats.TabularData()
        table.set_columns(headers)
        table.add_rows(list(r.values()) for r in records)
        render = table.render()

        fmt = f"```\n{render}\n```"
        if len(fmt) > 2000:
            fp = io.BytesIO(fmt.encode("utf-8"))
            await context.send("Too many results...", file=discord.File(fp, "results.txt"))
        else:
            await context.send(fmt)

    @group(name="commandhistory", hidden=True, invoke_without_command=True)
    @commands.is_owner()
    async def command_history(self, context):
        """Command history."""
        query = """SELECT
                        CASE failed
                            WHEN TRUE THEN command || ' [!]'
                            ELSE command
                        END AS "command",
                        to_char(used, 'Mon DD HH12:MI:SS AM') AS "invoked",
                        author_id,
                        guild_id
                   FROM commands
                   ORDER BY used DESC
                   LIMIT 15;
                """
        await self.tabulate_query(context, query)

    @command_history.command(name="for")
    @commands.is_owner()
    async def command_history_for(self, context, days: typing.Optional[int] = 7, *, command: str):
        """Command history for a command."""

        query = """SELECT *, t.success + t.failed AS "total"
                   FROM (
                       SELECT guild_id,
                              SUM(CASE WHEN failed THEN 0 ELSE 1 END) AS "success",
                              SUM(CASE WHEN failed THEN 1 ELSE 0 END) AS "failed"
                       FROM commands
                       WHERE command=$1
                       AND used > (CURRENT_TIMESTAMP - $2::interval)
                       GROUP BY guild_id
                   ) AS t
                   ORDER BY "total" DESC
                   LIMIT 30;
                """

        await self.tabulate_query(context, query, command, datetime.timedelta(days=days))

    @command_history.command(name="guild", aliases=["server"])
    @commands.is_owner()
    async def command_history_guild(self, context, guild_id: int):
        """Command history for a guild."""

        query = """SELECT
                        CASE failed
                            WHEN TRUE THEN command || ' [!]'
                            ELSE command
                        END AS "command",
                        channel_id,
                        author_id,
                        used
                   FROM commands
                   WHERE guild_id=$1
                   ORDER BY used DESC
                   LIMIT 15;
                """
        await self.tabulate_query(context, query, guild_id)

    @command_history.command(name="user", aliases=["member"])
    @commands.is_owner()
    async def command_history_user(self, context, user_id: int):
        """Command history for a user."""

        query = """SELECT
                        CASE failed
                            WHEN TRUE THEN command || ' [!]'
                            ELSE command
                        END AS "command",
                        guild_id,
                        used
                   FROM commands
                   WHERE author_id=$1
                   ORDER BY used DESC
                   LIMIT 20;
                """
        await self.tabulate_query(context, query, user_id)

    @command_history.command(name="log")
    @commands.is_owner()
    async def command_history_log(self, context, days=7):
        """Command history log for the last N days."""

        query = """SELECT command, COUNT(*)
                   FROM commands
                   WHERE used > (CURRENT_TIMESTAMP - $1::interval)
                   GROUP BY command
                   ORDER BY 2 DESC
                """

        all_commands = {c.qualified_name: 0 for c in self.bot.walk_commands()}

        records = await context.db.fetch(query, datetime.timedelta(days=days))
        for name, uses in records:
            if name in all_commands:
                all_commands[name] = uses

        as_data = sorted(all_commands.items(), key=lambda t: t[1], reverse=True)
        table = formats.TabularData()
        table.set_columns(["Command", "Uses"])
        table.add_rows(tup for tup in as_data)
        render = table.render()

        embed = discord.Embed(title="Summary", color=discord.Color.green())
        embed.set_footer(text="Since").timestamp = datetime.datetime.utcnow() - datetime.timedelta(days=days)

        top_ten = "\n".join(f"{command}: {uses}" for command, uses in records[:10])
        bottom_ten = "\n".join(f"{command}: {uses}" for command, uses in records[-10:])
        embed.add_field(name="Top 10", value=top_ten)
        embed.add_field(name="Bottom 10", value=bottom_ten)

        unused = ", ".join(name for name, uses in as_data if uses == 0)
        if len(unused) > 1024:
            unused = "Way too many..."

        embed.add_field(name="Unused", value=unused, inline=False)

        await context.send(
            embed=embed,
            file=discord.File(io.BytesIO(render.encode()), filename="full_results.txt"),
        )

    @command_history.command(name="cog")
    @commands.is_owner()
    async def command_history_cog(self, context, days: typing.Optional[int] = 7, *, cog: str = None):
        """Command history for a cog or grouped by a cog."""

        interval = datetime.timedelta(days=days)
        if cog is not None:
            cog = self.bot.get_cog(cog)
            if cog is None:
                return await context.send(f"Unknown cog: {cog}")

            query = """SELECT *, t.success + t.failed AS "total"
                       FROM (
                           SELECT command,
                                  SUM(CASE WHEN failed THEN 0 ELSE 1 END) AS "success",
                                  SUM(CASE WHEN failed THEN 1 ELSE 0 END) AS "failed"
                           FROM commands
                           WHERE command = any($1::text[])
                           AND used > (CURRENT_TIMESTAMP - $2::interval)
                           GROUP BY command
                       ) AS t
                       ORDER BY "total" DESC
                       LIMIT 30;
                    """
            return await self.tabulate_query(
                context,
                query,
                [c.qualified_name for c in cog.walk_commands()],
                interval,
            )

        # A more manual query with a manual grouper.
        query = """SELECT *, t.success + t.failed AS "total"
                   FROM (
                       SELECT command,
                              SUM(CASE WHEN failed THEN 0 ELSE 1 END) AS "success",
                              SUM(CASE WHEN failed THEN 1 ELSE 0 END) AS "failed"
                       FROM commands
                       WHERE used > (CURRENT_TIMESTAMP - $1::interval)
                       GROUP BY command
                   ) AS t;
                """

        class Count:
            __slots__ = ("success", "failed", "total")

            def __init__(self):
                self.success = 0
                self.failed = 0
                self.total = 0

            def add(self, record):
                self.success += record["success"]
                self.failed += record["failed"]
                self.total += record["total"]

        data = defaultdict(Count)
        records = await context.db.fetch(query, interval)
        for record in records:
            command = self.bot.get_command(record["command"])
            if command is None or command.cog is None:
                data["No Cog"].add(record)
            else:
                data[command.cog.qualified_name].add(record)

        table = formats.TabularData()
        table.set_columns(["Cog", "Success", "Failed", "Total"])
        data = sorted(
            [(cog, e.success, e.failed, e.total) for cog, e in data.items()],
            key=lambda t: t[-1],
            reverse=True,
        )

        table.add_rows(data)
        render = table.render()
        await context.safe_send(f"```\n{render}\n```")


old_on_error = commands.AutoShardedBot.on_error


async def on_error(self, event, *args, **kwargs):
    embed = discord.Embed(title="Event Error", color=0xA32952)
    embed.add_field(name="Event", value=event)
    embed.description = f"```py\n{traceback.format_exc()}\n```"
    embed.timestamp = datetime.datetime.now().astimezone()
    embed.set_footer(text=ttime.strftime("%B %d, %Y at %I:%M:%S %p %Z", ttime.localtime()))
    args_str = ["```py"]
    for index, arg in enumerate(args):
        args_str.append(f"[{index}]: {arg!r}")
    args_str.append("```")
    embed.add_field(name="Args", value="\n".join(args_str), inline=False)
    webhook = self.get_cog("Stats").webhook
    try:
        if isinstance(args[0], Context):
            if args[0].message.author.id == self.bot.owner_id:
                await args[0].send(embed=embed)
            else:
                await webhook.send(embed=embed)
        elif isinstance(args[0], discord.Message):
            msgContext = await self.get_context(args[0], cls=context)
            if args[0].author.id == self.bot.owner_id:
                await msgContext.send(embed=embed)
            else:
                await webhook.send(embed=embed)
    except:
        await webhook.send(embed=embed)
        pass


def setup(bot):
    if not hasattr(bot, "command_stats"):
        bot.command_stats = Counter()

    if not hasattr(bot, "socket_stats"):
        bot.socket_stats = Counter()

    cog = Stats(bot)
    bot.add_cog(cog)
    bot._stats_cog_gateway_handler = handler = GatewayHandler(cog)
    logging.getLogger().addHandler(handler)
    commands.AutoShardedBot.on_error = on_error


def teardown(bot):
    commands.AutoShardedBot.on_error = old_on_error
    logging.getLogger().removeHandler(bot._stats_cog_gateway_handler)
    del bot._stats_cog_gateway_handler
