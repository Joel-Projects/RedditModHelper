import asyncio
import datetime
import json
import sys
import traceback
from collections import defaultdict, deque
from functools import partial

import aiohttp
import asyncpg
import asyncpraw
import discord
from BotUtils import BotServices
from discord.ext import commands, tasks
from gitlab import Gitlab
from gitlab.v4.objects import Project

import config
from cogs.utils import context as context_cls
from cogs.utils.command_cog import CommandCog
from cogs.utils.config import Config
from cogs.utils.slash import SlashCommand

__version__ = "1.1.0"
bot_name = config.bot_name
description = "Hello! I am a bot written by Lil_SpazJoekp"

services = BotServices(bot_name)
log = services.logger()


initial_extensions = (
    "cogs.admin",
    "cogs.meta",
    "cogs.misc",
    "cogs.permissions",
    "cogs.reddit_stats",
    "cogs.settings",
    "cogs.stats",
    "cogs.sub_management",
)


def _prefix_callable(bot, msg):
    user_id = bot.user.id
    base = [f"<@!{user_id}> ", f"<@{user_id}> "]
    if msg.guild is None:
        base.append(".")
        base.append(",")
    else:
        base.extend(bot.prefixes.get(msg.guild.id, [".", ","]))
    return base


class RedditModHelper(commands.AutoShardedBot):
    def __init__(self, pool):
        allowed_mentions = discord.AllowedMentions(roles=True, everyone=True, users=True)
        intents = discord.Intents.all()
        super().__init__(
            command_prefix=_prefix_callable,
            description=description,
            pm_help=None,
            help_attrs=dict(hidden=True),
            fetch_offline_members=True,
            heartbeat_timeout=150.0,
            allowed_mentions=allowed_mentions,
            intents=intents,
        )
        self.debug = sys.platform == "darwin"
        self.client_id = config.client_id
        self.session = aiohttp.ClientSession(loop=self.loop)
        self._prev_events = deque(maxlen=10)
        self.owner_id = config.owner_id
        self.services = services
        self.credmgr = services.credmgr
        self.credmgr_bot = self.credmgr.bot(bot_name)
        self.reddit = services.reddit("Lil_SpazJoekp", asyncpraw=True)
        self.tempReddit = partial(self.switch_reddit_instance, bot=self)
        self.pool: asyncpg.pool.Pool = pool
        self.sql: asyncpg.pool.Pool = self.pool
        self.log = log
        self.running_tasks = {}
        self.snoo_guild: discord.Guild
        self.file_storage: discord.TextChannel
        gl = Gitlab("https://gitlab.jesassn.org", private_token=self.config.gitlab_token)
        self.gitlab_project: Project = gl.projects.get(143)
        # shard_id: List[datetime.datetime]
        # shows the last attempted IDENTIFYs and RESUMEs
        self.resumes = defaultdict(list)
        self.identifies = defaultdict(list)
        self.prefixes = Config("prefixes.json")
        self.blacklist = Config("blacklist.json")
        self.slash = SlashCommand(self, sync_commands=True, debug_guild=785198941535731715)
        for extension in initial_extensions:
            try:
                self.load_extension(extension)
            except Exception as error:
                log.error(f"Failed to load extension {extension}. Error: {error}")
                traceback.print_exc()

    @staticmethod
    def get_reddit(username):
        return asyncpraw.Reddit(**services.reddit(username).config._settings)

    def _clear_gateway_data(self):
        one_week_ago = datetime.datetime.utcnow() - datetime.timedelta(days=7)
        for shard_id, dates in self.identifies.items():
            to_remove = [index for index, dt in enumerate(dates) if dt < one_week_ago]
            for index in reversed(to_remove):
                del dates[index]

        for shard_id, dates in self.resumes.items():
            to_remove = [index for index, dt in enumerate(dates) if dt < one_week_ago]
            for index in reversed(to_remove):
                del dates[index]

    async def on_socket_response(self, msg):
        self._prev_events.append(msg)

    async def before_identify_hook(self, shard_id, *, initial):
        self._clear_gateway_data()
        self.identifies[shard_id].append(datetime.datetime.utcnow())
        await super().before_identify_hook(shard_id, initial=initial)

    async def on_command_error(self, context, error):
        if isinstance(error, commands.NoPrivateMessage):
            await CommandCog.error_embed(context, "This command cannot be used in private messages.")
        elif isinstance(error, commands.DisabledCommand):
            await CommandCog.error_embed(context, "Sorry. This command is disabled and cannot be used.")
        elif isinstance(error, commands.CheckFailure):
            await CommandCog.error_embed(context, "You're not allowed to do that!")
        elif isinstance(error, commands.CommandInvokeError):
            self.log_error(context, error)
        elif isinstance(error, (commands.ArgumentParsingError, commands.MissingRequiredArgument)):
            await context.send(error)
        elif isinstance(error, commands.MissingRequiredArgument):
            await context.send_help(context.command)
            await context.send(error)
        else:
            self.log_error(context, error)

    async def on_slash_command_error(self, context, error):
        self.log_error(context, error)

    def log_error(self, context, error):
        try:
            log.error(f"In {context.command}:\n{error.original.__class__.__name__}: {error.original}")
            traceback.print_tb(error.original.__traceback__)
        except AttributeError:
            log.error(f"In {context.command}:\n{error.__class__.__name__}: {error}")
            traceback.print_tb(error.__traceback__)

    def get_guild_prefixes(self, guild, *, local_inject=_prefix_callable):
        proxy_msg = discord.Object(id=0)
        proxy_msg.guild = guild
        return local_inject(self, proxy_msg)

    def get_raw_guild_prefixes(self, guild_id):
        return self.prefixes.get(guild_id, [",", "."])

    async def set_guild_prefixes(self, guild, prefixes):
        if len(prefixes) == 0:
            await self.prefixes.put(guild.id, [])
        elif len(prefixes) > 10:
            raise RuntimeError("Cannot have more than 10 custom prefixes.")
        else:
            await self.prefixes.put(guild.id, sorted(set(prefixes), reverse=True))

    async def query_member_named(self, guild, argument, *, cache=False):
        """Queries a member by their name, name + discrim, or nickname.

        Parameters
        ------------
        guild: Guild
            The guild to query the member in.
        argument: str
            The name, nickname, or name + discrim combo to check.
        cache: bool
            Whether to cache the results of the query.

        Returns
        ---------
        Optional[Member]
            The member matching the query or None if not found.
        """
        if len(argument) > 5 and argument[-5] == "#":
            username, _, discriminator = argument.rpartition("#")
            members = await guild.query_members(username, limit=100, cache=cache)
            return discord.utils.get(members, name=username, discriminator=discriminator)
        else:
            members = await guild.query_members(argument, limit=100, cache=cache)
            return discord.utils.find(lambda m: m.name == argument or m.nick == argument, members)

    async def get_or_fetch_member(self, guild, member_id):
        """Looks up a member in cache or fetches if not found.

        Parameters
        ----------
        guild: Guild
            The guild to look in.
        member_id: int
            The member ID to search for.

        Returns
        -------
        Optional[Member]
            The member or None if not found.
        """

        member = guild.get_member(
            member_id,
        )
        if member is not None:
            return member

        shard = self.get_shard(guild.shard_id)
        if shard.is_ws_ratelimited():
            try:
                member = await guild.fetch_member(member_id)
            except discord.HTTPException:
                return None
            else:
                return member

        members = await guild.query_members(limit=1, user_ids=[member_id], cache=True)
        if not members:
            return None
        return members[0]

    async def resolve_member_ids(self, guild, member_ids):
        """Bulk resolve member IDs to member instances, if possible.

        Members that can't be resolved are discarded from the list.

        This is done lazily using an asynchronous iterator.

        Note that the order of the resolved members is not the same as the input.

        Parameters
        -----------
        guild: Guild
            The guild to resolve from.
        member_ids: Iterable[int]
            An iterable of member IDs.

        Yields
        --------
        Member
            The resolved members.
        """

        needs_resolution = []
        for member_id in member_ids:
            member = guild.get_member(
                member_id,
            )
            if member is not None:
                yield member
            else:
                needs_resolution.append(member_id)

        total_need_resolution = len(needs_resolution)
        if total_need_resolution == 1:
            shard = self.get_shard(guild.shard_id)
            if shard.is_ws_ratelimited():
                try:
                    member = await guild.fetch_member(needs_resolution[0])
                except discord.HTTPException:
                    pass
                else:
                    yield member
            else:
                members = await guild.query_members(limit=1, user_ids=needs_resolution, cache=True)
                if members:
                    yield members[0]
        elif total_need_resolution <= 100:
            # Only a single resolution call needed here
            resolved = await guild.query_members(limit=100, user_ids=needs_resolution, cache=True)
            for member in resolved:
                yield member
        else:
            # We need to chunk these in bits of 100...
            for index in range(0, total_need_resolution, 100):
                to_resolve = needs_resolution[index : index + 100]
                resolved = await guild.query_members(limit=100, user_ids=to_resolve, cache=True)
                for member in resolved:
                    yield member

    @tasks.loop(seconds=1200.0)
    async def print_servers(self):
        if not self.is_closed():
            for server in self.guilds:
                self.log.info(server.name)

    async def on_ready(self):
        if not hasattr(self, "uptime"):
            self.uptime = datetime.datetime.utcnow()
        log.info(f"Ready: {self.user} (ID: {self.user.id})")
        self.snoo_guild: discord.Guild = self.get_guild(785198941535731715)
        self.file_storage: discord.TextChannel = self.get_channel(824789213651271710)
        if not self.print_servers.is_running():
            self.print_servers.start()

    class switch_reddit_instance:
        def __init__(self, user, bot):
            self.user = user
            self.bot = bot

        def __enter__(self):
            log.debug(f"Switching to u/{self.user}")
            return self.bot.services.reddit(self.user, asyncpraw=True)

        def __exit__(self, exc_type, exc_val, exc_tb):
            log.debug("Switching back to u/Lil_SpazJoekp")

    async def on_shard_resumed(self, shard_id):
        print(f"Shard ID {shard_id} has resumed...")
        self.resumes[shard_id].append(datetime.datetime.utcnow())

    @discord.utils.cached_property
    def stats_webhook(self):
        wh_id, wh_token = self.config.stat_webhook
        hook = discord.Webhook.partial(id=wh_id, token=wh_token, adapter=discord.AsyncWebhookAdapter(self.session))
        return hook

    async def process_commands(self, message):

        context = await self.get_context(message, cls=context_cls.Context)

        if context.command is None:
            return
        if context.guild is not None:
            if context.guild.id in self.blacklist:
                return
            if context.guild.id != 785198941535731715:
                if not self.debug:
                    if context.author.id != self.owner_id:
                        return

        await self.invoke(context)
        if len(self.running_tasks.keys()) > 0:
            done = False
            tasks = [task for user in self.running_tasks.keys() for task in self.running_tasks[user]]
            while not done:
                await asyncio.sleep(1)
                done = all([task.done() for task in tasks])
        await context.release()

    async def on_message(self, message):
        await config.check_message(self, message)
        if message.author.bot:
            return
        if self.debug and message.author.id != self.owner_id:
            return
        await self.process_commands(message)

    def start_task(self, context, func, *, taskName=None):
        user = context.author.id
        task = self.loop.create_task(func)

        task.add_done_callback(partial(self.task_completion_handler, task=task, user=user, context=context))
        if not user in self.running_tasks:
            self.running_tasks[user] = {}
        self.running_tasks[user][task] = taskName or context.message.content.replace(
            context.invoked_with, context.command.name
        )
        return task

    def task_completion_handler(self, task, user, context):
        tasks = self.running_tasks.get(user, None)
        if tasks:
            if task in tasks:
                self.running_tasks[user].pop(task)
                if len(self.running_tasks[user]) == 0:
                    self.running_tasks.pop(user)

    async def close(self):
        await super().close()
        await self.session.close()

    def run(self):
        try:
            super().run(config.token, reconnect=True)
        finally:
            with open("prev_events.log", "w", encoding="utf-8") as fp:
                for data in self._prev_events:
                    try:
                        x = json.dumps(data, ensure_ascii=True, indent=4)
                    except Exception:
                        fp.write(f"{data}\n")
                    else:
                        fp.write(f"{x}\n")

    @property
    def config(self):
        return __import__("config")
