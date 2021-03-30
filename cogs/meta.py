import asyncio
import copy
import datetime
import string
import sys
import time
import unicodedata
from collections import Counter
from typing import Union

import dateparser as dateparser
import dateutil
import discord
from discord import Embed
from discord.ext import commands, menus
from discord_slash.utils.manage_commands import create_choice, create_option
from gitlab import Gitlab

import config

from .utils import checks, formats
from .utils import time
from .utils import time as utime
from .utils.command_cog import CommandCog
from .utils.commands import command, group
from .utils.paginator import RoboPages
from .utils.slash import cog_slash
from .utils.utils import ordinal


class Prefix(commands.Converter):
    async def convert(self, context, argument):
        user_id = context.bot.user.id
        if argument.startswith((f"<@{user_id}>", f"<@!{user_id}>")):
            raise commands.BadArgument("That is a reserved prefix already in use.")
        return argument


class BotHelpPageSource(menus.ListPageSource):
    def __init__(self, help_command, commands):
        # entries = [(cog, len(sub)) for cog, sub in commands.items()]
        # entries.sort(key=lambda t: (t[0].qualified_name, t[1]), reverse=True)
        super().__init__(entries=sorted(commands.keys(), key=lambda c: c.qualified_name), per_page=6)
        self.commands = commands
        self.help_command = help_command
        self.prefix = help_command.clean_prefix

    def format_commands(self, cog, commands):
        # A field can only have 1024 characters so we need to paginate a bit
        # just in case it doesn't fit perfectly
        # However, we have 6 per page so I'll try cutting it off at around 800 instead
        # Since there's a 6000 character limit overall in the embed
        if cog.description:
            short_doc = cog.description.split("\n", 1)[0] + "\n"
        else:
            short_doc = "No help found...\n"

        current_count = len(short_doc)
        ending_note = "+%d not shown"
        ending_length = len(ending_note)

        page = []
        for command in commands:
            value = f"`{command.name}`"
            count = len(value) + 1  # The space
            if count + current_count < 800:
                current_count += count
                page.append(value)
            else:
                # If we're maxed out then see if we can add the ending note
                if current_count + ending_length + 1 > 800:
                    # If we are, pop out the last element to make room
                    page.pop()

                # Done paginating so just exit
                break

        if len(page) == len(commands):
            # We're not hiding anything so just return it as-is
            return short_doc + " ".join(page)

        hidden = len(commands) - len(page)
        return f"{short_doc}{' '.join(page)}\n{(ending_note % hidden)}"

    async def format_page(self, menu: menus.Menu, cogs):
        prefix = menu.ctx.prefix
        description = (
            f'Use "{prefix}help command" for more info on a command.\n'
            f'Use "{prefix}help category" for more info on a category.\n'
            "For more help, Contact <@393801572858986496>."
        )

        embed = discord.Embed(title="Categories", description=description, color=discord.Color.blurple())

        for cog in cogs:
            commands = self.commands.get(cog)
            if commands:
                value = self.format_commands(cog, commands)
                embed.add_field(name=cog.qualified_name, value=value, inline=True)

        maximum = self.get_max_pages()
        embed.set_footer(text=f"Page {menu.current_page + 1}/{maximum}")
        return embed


class GroupHelpPageSource(menus.ListPageSource):
    def __init__(self, group, commands, *, prefix):
        super().__init__(entries=commands, per_page=6)
        self.group = group
        self.prefix = prefix
        self.title = f"{self.group.qualified_name} Commands"
        self.description = self.group.description

    async def format_page(self, menu, commands):
        embed = discord.Embed(
            title=self.title,
            description=self.description,
            color=discord.Color.blurple(),
        )

        for command in commands:
            signature = f"{command.qualified_name} {command.signature}"
            embed.add_field(
                name=signature,
                value=command.short_doc or "No help given...",
                inline=False,
            )

        maximum = self.get_max_pages()
        if maximum > 1:
            embed.set_author(name=f"Page {menu.current_page + 1}/{maximum} ({len(self.entries)} commands)")

        embed.set_footer(text=f'Use "{self.prefix}help command" for more info on a command.')
        return embed


class HelpMenu(RoboPages):
    def __init__(self, source):
        super().__init__(source)

    @menus.button("\N{WHITE QUESTION MARK ORNAMENT}", position=menus.Last(5))
    async def show_bot_help(self, payload):
        """shows how to use the bot"""

        embed = discord.Embed(title="Using the bot", color=discord.Color.blurple())
        embed.title = "Using the bot"
        embed.description = "Hello! Welcome to the help page."

        entries = (
            ("<argument>", "This means the argument is __**required**__."),
            ("[argument]", "This means the argument is __**optional**__."),
            ("[A|B]", "This means that it can be __**either A or B**__."),
            (
                "[argument...]",
                "This means you can have multiple arguments.\n"
                "Now that you know the basics, it should be noted that...\n"
                "__**You do not type in the brackets!**__",
            ),
        )

        embed.add_field(
            name="How do I use this bot?",
            value="Reading the bot signature is pretty simple.",
        )

        for name, value in entries:
            embed.add_field(name=name, value=value, inline=False)

        embed.set_footer(text=f"We were on page {self.current_page + 1} before this message.")
        await self.message.edit(embed=embed)

        async def go_back_to_current_page():
            await asyncio.sleep(30.0)
            await self.show_page(self.current_page)

        self.bot.loop.create_task(go_back_to_current_page())


class PaginatedHelpCommand(commands.HelpCommand):
    def __init__(self):
        super().__init__(
            command_attrs={
                "cooldown": commands.Cooldown(1, 3.0, commands.BucketType.member),
                "help": "Shows help about the bot, a command, or a category",
                **({"aliases": ["help2"]} if sys.platform == "darwin" else {}),
            }
        )

    async def on_help_command_error(self, context, error):
        if isinstance(error, commands.CommandInvokeError):
            await context.send(str(error.original))

    def get_command_signature(self, command):
        parent = command.full_parent_name
        if len(command.aliases) > 0:
            aliases = "|".join(command.aliases)
            fmt = f"[{command.name}|{aliases}]"
            if parent:
                fmt = f"{parent} {fmt}"
            alias = fmt
        else:
            alias = command.name if not parent else f"{parent} {command.name}"
        return f"{alias} {command.signature}"

    async def send_bot_help(self, mapping):
        bot = self.context.bot
        entries = await self.filter_commands(bot.commands, sort=True)

        all_commands = {}
        for command in entries:
            if command.cog is None:
                continue
            try:
                all_commands[command.cog].append(command)
            except KeyError:
                all_commands[command.cog] = [command]

        menu = HelpMenu(BotHelpPageSource(self, all_commands))
        await self.context.release()
        await menu.start(self.context)

    async def send_cog_help(self, cog):
        entries = await self.filter_commands(cog.get_commands(), sort=True)
        menu = HelpMenu(GroupHelpPageSource(cog, entries, prefix=self.clean_prefix))
        await self.context.release()
        await menu.start(self.context)

    def parse_date(self, date: str = None):
        """
        :param date:
        :return datetime.datetime: datetime object
        """
        settings = dateparser.conf.Settings()
        settings.PREFER_DAY_OF_MONTH = "first"
        settings.RETURN_AS_TIMEZONE_AWARE = True
        settings.TIMEZONE = "UTC"
        settings.PREFER_DATES_FROM = "past"
        try:
            parsedDate = dateparser.parse(date)
            return parsedDate
        except Exception:
            return

    def checkDate(self, date, *, lastMonth=False, currentMonth=False, today=False):
        if date:
            if isinstance(date, int):
                if 1 <= date <= 12:
                    parsedDate = self.parse_date(date)
            else:
                parsedDate = self.parse_date(date)
        else:
            if today:
                parsedDate = self.parse_date(time.strftime("%m/%d/%Y %I:%M:%S %p", time.localtime()))
            else:
                if lastMonth:
                    months = 1
                if currentMonth:
                    months = 0
                parsedDate = self.parse_date(
                    time.strftime(
                        "%m",
                        time.gmtime(
                            datetime.timestamp(datetime.today() - dateutil.relativedelta.relativedelta(months=months))
                        ),
                    )
                )
        return parsedDate

    def getDateStr(self, dateStr, **kwargs):
        date = self.checkDate(dateStr, **kwargs)
        return date.strftime(f"%b {ordinal(date.day)}, %Y at %I:%M %p")

    def common_command_formatting(self, page_or_embed, command):
        page_or_embed.title = self.get_command_signature(command)
        commandHelp = command.help
        if command.name == "matrix" or command.name == "tbmatrix":
            commandHelp = command.help.format(
                **{
                    "none"
                    if dateStr == "none"
                    else dateStr: self.getDateStr(None, currentMonth=True)
                    if dateStr == "none"
                    else self.getDateStr(dateStr.replace("_", " "))
                    for dateStr in [tup[1] for tup in string.Formatter().parse(command.help) if tup[1] is not None]
                }
            )
        if command.description:
            page_or_embed.description = f"{command.description}\n\n{commandHelp}"
        else:
            page_or_embed.description = commandHelp or "No help found..."

    async def send_command_help(self, command):
        # No pagination necessary for a single command.
        embed = discord.Embed(color=discord.Color.blurple())
        self.common_command_formatting(embed, command)
        await self.context.send(embed=embed)

    async def send_group_help(self, group):
        subcommands = group.commands
        if len(subcommands) == 0:
            return await self.send_command_help(group)

        entries = await self.filter_commands(subcommands, sort=True)
        if len(entries) == 0:
            return await self.send_command_help(group)

        source = GroupHelpPageSource(group, entries, prefix=self.clean_prefix)
        self.common_command_formatting(source, group)
        menu = HelpMenu(source)
        await self.context.release()
        await menu.start(self.context)


class Meta(CommandCog):
    """Commands for utilities related to Discord or the Bot itself."""

    def __init__(self, bot):
        super().__init__(bot)
        self.old_help_command = bot.help_command
        bot.help_command = PaginatedHelpCommand()
        bot.help_command.cog = self

    def cog_unload(self):
        self.bot.help_command = self.old_help_command

    async def cog_command_error(self, context, error):
        if isinstance(error, commands.BadArgument):
            await context.send(error)
        self.log.error(error)

    @cog_slash(
        options=[
            create_option(
                "feedback_type",
                "Type of feedback.",
                str,
                True,
                [
                    create_choice(name=label.name, value=label.name)
                    for label in Gitlab("https://gitlab.jesassn.org", private_token=config.gitlab_token)
                    .projects.get(143)
                    .labels.list()
                ],
            ),
            create_option("title", "Feedback title", str, True),
            create_option("extra_details", "Optional extra feedback info.", str, False),
        ],
    )
    async def feedback(self, context, feedback_type, title, extra_details=None):
        """Give feedback to the author of the bot."""
        await context.defer()

        data = {
            "title": f"{context.author.name}: {title.title()}",
            "description": f"{extra_details}\n\n{context.message}",
            "labels": [feedback_type],
        }
        issue = self.bot.gitlab_project.issues.create(data)
        await self.success_embed(context, "Successfully sent feedback!")

    @command(aliases=["rc"])
    async def runningcommands(self, context):
        tasks = copy.copy(self.bot.running_tasks)
        is_admin = await checks.check_guild_permissions(context, {"administrator": True})
        running_tasks = {}
        if is_admin:
            embed = Embed(title="Running Commands", color=discord.Color.purple())
            i = 0
            for user, tasks in tasks.items():
                username = discord.utils.get(context.guild.members, id=user)
                strings = []
                for task in tasks.items():
                    if not task[0].done():
                        i += 1
                        strings.append(f"{i}. {task[1]}")
                    else:
                        try:
                            self.bot.running_tasks[user].pop(task)
                            if len(self.bot.running_tasks[user]) == 0:
                                self.bot.running_tasks.pop(user)
                        except ValueError:
                            pass
                i = 0
                value_string = "\n".join(strings)
                embed.add_field(name=username, value=value_string)
        else:
            embed = Embed(title="Your Running Commands", color=discord.Color.purple())
            strings = []
            for i, task in enumerate(self.bot.running_tasks[context.author.id].items(), 1):
                strings.append(f"{i}. {task[1]}")
                running_tasks[i] = task[0]
            value_string = "\n".join(strings)
            embed.add_field(name="Commands", value=value_string)
        embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
        await context.send(embed=embed)

    @command(aliases=["c"])
    async def cancel(self, context, *commands):
        tasks = copy.copy(self.bot.running_tasks)
        is_admin = await checks.check_guild_permissions(context, {"administrator": True})
        running_tasks = {}
        strings = {}
        i = 0
        description = "Please list the numbers for the commands you want to cancel"
        if is_admin:
            embed = Embed(
                title="Running Commands",
                color=discord.Color.blurple(),
                description=description,
            )
        else:
            embed = Embed(
                title="Your Running Commands",
                color=discord.Color.blurple(),
                description=description,
            )

        for user, userTasks in self.bot.running_tasks.items():
            if is_admin or user == context.author.id:
                if is_admin:
                    username = discord.utils.get(context.guild.members, id=user)
                else:
                    username = "Commands"
            else:
                continue
            try:
                for task in userTasks.items():
                    if not task[0].done():
                        i += 1
                        if not username in strings:
                            strings[username] = []
                        strings[username].append(f"{i}. {task[1]}")
                        running_tasks[i - 1] = task[0]
                    else:
                        try:
                            tasks[user].pop(task[0])
                            if len(tasks[user]) == 0:
                                tasks.pop(user)
                        except ValueError:
                            pass
            except RuntimeError:
                await context.reinvoke()
                return
        for user, value_string in strings.items():
            embed.add_field(name=user, value="\n".join(value_string))
        if commands:
            cancelAll = "all" in [i.lower() for i in commands]
            if cancelAll:
                embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
                await context.send(embed=embed)
                confirm = await context.prompt("Are you sure you want to cancel the above commands?")
                if confirm:
                    for task in running_tasks.values():
                        try:
                            task.cancel()
                        except Exception as error:
                            await self.error_embed(context, error)
                            return
        else:
            toCancel = await self.prompt_options(
                context,
                "Please select which command(s) to cancel",
                "List the number of the command(s) separated with spaces",
                embed.fields,
                "fields",
                multiSelect=True,
            )
            if toCancel:
                for command in toCancel:
                    task = running_tasks[command]
                    try:
                        task.cancel()
                    except Exception as error:
                        await self.error_embed(context, error)
                        return
            else:
                return
        await self.success_embed(context, "Command(s) cancelled successfully")

    @command()
    async def charinfo(self, context, *, characters: str):
        """Shows you information about a number of characters.

        Only up to 25 characters at a time.
        """

        def to_string(c):
            digit = f"{ord(c):x}"
            name = unicodedata.name(c, "Name not found.")
            return f"`\\U{digit:>08}`: {name} - {c} \N{EM DASH} <https://www.fileformat.info/info/unicode/char/{digit}>"

        msg = "\n".join(map(to_string, characters))
        if len(msg) > 2000:
            return await context.send("Output too long to display.")
        await context.send(msg)

    @group(name="prefix", invoke_without_command=True)
    async def prefix(self, context):
        """Manages the server's custom prefixes.

        If called without a subcommand, this will list the currently set
        prefixes.
        """

        prefixes = self.bot.get_guild_prefixes(context.guild)

        # we want to remove prefix #2, because it's the 2nd form of the mention
        # and to the end user, this would end up making them confused why the
        # mention is there twice
        del prefixes[1]

        embed = discord.Embed(title="Prefixes", color=discord.Color.blurple())
        embed.set_footer(text=f"{len(prefixes)} prefixes")
        embed.description = "\n".join(f"{index}. {elem}" for index, elem in enumerate(prefixes, 1))
        await context.send(embed=embed)

    @prefix.command(name="add", ignore_extra=False)
    @checks.is_mod()
    async def prefix_add(self, context, prefix: Prefix):
        """Appends a prefix to the list of custom prefixes.

        Previously set prefixes are not overridden.

        To have a word prefix, you should quote it and end it with
        a space, e.g. "hello " to set the prefix to "hello ". This
        is because Discord removes spaces when sending messages so
        the spaces are not preserved.

        Multi-word prefixes must be quoted also.

        You must have Manage Server permission to use this command.
        """

        current_prefixes = self.bot.get_raw_guild_prefixes(context.guild.id)
        current_prefixes.append(prefix)
        try:
            await self.bot.set_guild_prefixes(context.guild, current_prefixes)
        except Exception as error:
            await context.send(f"{context.tick(False)} {error}")
        else:
            await context.send(context.tick(True))

    @prefix_add.error
    async def prefix_add_error(self, context, error):
        if isinstance(error, commands.TooManyArguments):
            await context.send("You've given too many prefixes. Either quote it or only do it one by one.")

    @prefix.command(name="remove", aliases=["delete"], ignore_extra=False)
    @checks.is_mod()
    async def prefix_remove(self, context, prefix: Prefix):
        """Removes a prefix from the list of custom prefixes.

        This is the inverse of the 'prefix add' command. You can
        use this to remove prefixes from the default set as well.

        You must have Manage Server permission to use this command.
        """

        current_prefixes = self.bot.get_raw_guild_prefixes(context.guild.id)

        try:
            current_prefixes.remove(prefix)
        except ValueError:
            return await context.send("I do not have this prefix registered.")

        try:
            await self.bot.set_guild_prefixes(context.guild, current_prefixes)
        except Exception as error:
            await context.send(f"{context.tick(False)} {error}")
        else:
            await context.send(context.tick(True))

    @prefix.command(name="clear")
    @checks.is_mod()
    async def prefix_clear(self, context):
        """Removes all custom prefixes.

        After this, the bot will listen to only mention prefixes.

        You must have Manage Server permission to use this command.
        """

        await self.bot.set_guild_prefixes(context.guild, [])
        await context.send(context.tick(True))

    @command(name="quit", aliases=["kill"], hidden=True)
    @commands.is_owner()
    async def _quit(self, context):
        """Quits the bot."""
        embed = Embed(description="Shutting down...")
        await context.send(embed=embed)
        await self.bot.logout()
        exit()

    @command()
    async def avatar(self, context, *, user: Union[discord.Member, discord.User] = None):
        """Shows a user's enlarged avatar (if possible)."""
        embed = discord.Embed()
        user = user or context.author
        avatar = user.avatar_url_as(static_format="png")
        embed.set_author(name=str(user), url=avatar)
        embed.set_image(url=avatar)
        await context.send(embed=embed)

    @command()
    async def info(self, context, *, user: Union[discord.Member, discord.User] = None):
        """Shows info about a user."""

        user = user or context.author
        if context.guild and isinstance(user, discord.User):
            user = (
                context.guild.get_member(
                    user.id,
                )
                or user
            )

        embed = discord.Embed()
        roles = [role.name.replace("@", "@\u200b") for role in getattr(user, "roles", [])]
        shared = sum(
            guild.get_member(
                user.id,
            )
            is not None
            for guild in self.bot.guilds
        )
        embed.set_author(name=str(user))

        def format_date(dt):
            if dt is None:
                return "N/A"
            return f"{dt:%m-%d-%Y %I:%M %p} ({utime.human_timedelta(dt, accuracy=3)})"

        embed.add_field(name="ID", value=user.id, inline=False)
        embed.add_field(name="Servers", value=f"{shared} shared", inline=False)
        embed.add_field(
            name="Joined",
            value=format_date(getattr(user, "joined_at", None)),
            inline=False,
        )
        embed.add_field(name="Created", value=format_date(user.created_at), inline=False)

        voice = getattr(user, "voice", None)
        if voice is not None:
            vc = voice.channel
            other_people = len(vc.members) - 1
            voice = f"{vc.name} with {other_people} others" if other_people else f"{vc.name} by themselves"
            embed.add_field(name="Voice", value=voice, inline=False)

        if roles:
            embed.add_field(
                name="Roles",
                value=", ".join(roles) if len(roles) < 10 else f"{len(roles)} roles",
                inline=False,
            )

        color = user.color
        if color.value:
            embed.color = color

        if user.avatar:
            embed.set_thumbnail(url=user.avatar_url)

        if isinstance(user, discord.User):
            embed.set_footer(text="This member is not in this server.")

        await context.send(embed=embed)

    @command(aliases=["guildinfo"], usage="")
    @commands.guild_only()
    async def serverinfo(self, context, *, guild_id: int = None):
        """Shows info about the current server."""

        if guild_id is not None and await self.bot.is_owner(context.author):
            guild = self.bot.get_guild(guild_id)
            if guild is None:
                return await context.send(f"Invalid Guild ID given.")
        else:
            guild = context.guild

        roles = [role.name.replace("@", "@\u200b") for role in guild.roles]

        if not guild.chunked:
            async with context.typing():
                await guild.chunk(cache=True)

        # figure out what channels are 'secret'
        everyone = guild.default_role
        everyone_perms = everyone.permissions.value
        secret = Counter()
        totals = Counter()
        for channel in guild.channels:
            allow, deny = channel.overwrites_for(everyone).pair()
            perms = discord.Permissions((everyone_perms & ~deny.value) | allow.value)
            channel_type = type(channel)
            totals[channel_type] += 1
            if not perms.read_messages:
                secret[channel_type] += 1
            elif isinstance(channel, discord.VoiceChannel) and (not perms.connect or not perms.speak):
                secret[channel_type] += 1

        embed = discord.Embed()
        embed.title = guild.name
        embed.description = f"**ID**: {guild.id}\n**Owner**: {guild.owner}"
        if guild.icon:
            embed.set_thumbnail(url=guild.icon_url)

        channel_info = []
        key_to_emoji = {
            discord.TextChannel: "<:text_channel:646449055177637931>",
            discord.VoiceChannel: "<:voice_channel:646449055089557525>",
        }
        for key, total in totals.items():
            secrets = secret[key]
            try:
                emoji = key_to_emoji[key]
            except KeyError:
                continue

            if secrets:
                channel_info.append(f"{emoji} {total} ({secrets} locked)")
            else:
                channel_info.append(f"{emoji} {total}")

        info = []
        features = set(guild.features)
        all_features = {
            "PARTNERED": "Partnered",
            "VERIFIED": "Verified",
            "DISCOVERABLE": "Server Discovery",
            "COMMUNITY": "Community Server",
            "FEATURABLE": "Featured",
            "WELCOME_SCREEN_ENABLED": "Welcome Screen",
            "INVITE_SPLASH": "Invite Splash",
            "VIP_REGIONS": "VIP Voice Servers",
            "VANITY_URL": "Vanity Invite",
            "COMMERCE": "Commerce",
            "LURKABLE": "Lurkable",
            "NEWS": "News Channels",
            "ANIMATED_ICON": "Animated Icon",
            "BANNER": "Banner",
        }

        for feature, label in all_features.items():
            if feature in features:
                info.append(f"{context.tick(True)}: {label}")

        if info:
            embed.add_field(name="Features", value="\n".join(info))

        embed.add_field(name="Channels", value="\n".join(channel_info))

        if guild.premium_tier != 0:
            boosts = f"Level {guild.premium_tier}\n{guild.premium_subscription_count} boosts"
            last_boost = max(guild.members, key=lambda m: m.premium_since or guild.created_at)
            if last_boost.premium_since is not None:
                boosts = f"{boosts}\nLast Boost: {last_boost} ({utime.human_timedelta(last_boost.premium_since, accuracy=2)})"
            embed.add_field(name="Boosts", value=boosts, inline=False)

        bots = sum(m.bot for m in guild.members)
        fmt = f"Total: {guild.member_count} ({formats.plural(bots):bot})"

        embed.add_field(name="Members", value=fmt, inline=False)
        embed.add_field(
            name="Roles",
            value=", ".join(roles) if len(roles) < 10 else f"{len(roles)} roles",
        )

        emoji_stats = Counter()
        for emoji in guild.emojis:
            if emoji.animated:
                emoji_stats["animated"] += 1
                emoji_stats["animated_disabled"] += not emoji.available
            else:
                emoji_stats["regular"] += 1
                emoji_stats["disabled"] += not emoji.available

        fmt = (
            f'Regular: {emoji_stats["regular"]}/{guild.emoji_limit}\n'
            f'Animated: {emoji_stats["animated"]}/{guild.emoji_limit}\n'
        )
        if emoji_stats["disabled"] or emoji_stats["animated_disabled"]:
            fmt = f'{fmt}Disabled: {emoji_stats["disabled"]} regular, {emoji_stats["animated_disabled"]} animated\n'

        fmt = f"{fmt}Total Emoji: {len(guild.emojis)}/{guild.emoji_limit*2}"
        embed.add_field(name="Emoji", value=fmt, inline=False)
        embed.set_footer(text="Created").timestamp = guild.created_at
        await context.send(embed=embed)

    async def say_permissions(self, context, member, channel):
        permissions = channel.permissions_for(member)
        embed = discord.Embed(color=member.color)
        avatar = member.avatar_url_as(static_format="png")
        embed.set_author(name=str(member), url=avatar)
        allowed, denied = [], []
        for name, value in permissions:
            name = name.replace("_", " ").replace("guild", "server").title()
            if value:
                allowed.append(name)
            else:
                denied.append(name)

        embed.add_field(name="Allowed", value="\n".join(allowed))
        embed.add_field(name="Denied", value="\n".join(denied))
        await context.send(embed=embed)

    @command()
    @commands.guild_only()
    async def permissions(
        self,
        context,
        member: discord.Member = None,
        channel: discord.TextChannel = None,
    ):
        """Shows a member's permissions in a specific channel.

        If no channel is given then it uses the current one.

        You cannot use this in private messages. If no member is given then
        the info returned will be yours.
        """
        channel = channel or context.channel
        if member is None:
            member = context.author

        await self.say_permissions(context, member, channel)

    @command()
    @commands.guild_only()
    @checks.admin_or_permissions(manage_roles=True)
    async def botpermissions(self, context, *, channel: discord.TextChannel = None):
        """Shows the bot's permissions in a specific channel.

        If no channel is given then it uses the current one.

        This is a good way of checking if the bot has the permissions needed
        to execute the commands it wants to execute.

        To execute this command you must have Manage Roles permission.
        You cannot use this in private messages.
        """
        channel = channel or context.channel
        member = context.guild.me
        await self.say_permissions(context, member, channel)

    @command()
    @commands.is_owner()
    async def debugpermissions(self, context, guild_id: int, channel_id: int, author_id: int = None):
        """Shows permission resolution for a channel and an optional author."""

        guild = self.bot.get_guild(guild_id)
        if guild is None:
            return await context.send("Guild not found?")

        channel = guild.get_channel(channel_id)
        if channel is None:
            return await context.send("Channel not found?")

        if author_id is None:
            member = guild.me
        else:
            member = self.bot.get_or_fetch_member(guild, author_id)

        if member is None:
            return await context.send("Member not found?")

        await self.say_permissions(context, member, channel)

    @command(rest_is_raw=True, hidden=True)
    @commands.is_owner()
    async def echo(self, context, *, content):
        await context.send(content)

    @command(hidden=True)
    async def cud(self, context):
        """pls no spam"""

        for i in range(3):
            await context.send(3 - i)
            await asyncio.sleep(1)

        await context.send("go")


def setup(bot):
    bot.add_cog(Meta(bot))
