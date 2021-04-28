"""Commands for getting various Reddit Moderation statistics"""
import enum
import io
import time
from asyncio import CancelledError
from datetime import datetime
from enum import auto
from functools import cached_property
from typing import NamedTuple

import asyncpg
import asyncprawcore
import dataframe_image
import dateparser
import discord
import pandas
import pytz
from asyncpraw.exceptions import InvalidURL
from asyncpraw.models import Subreddit
from asyncprawcore import NotFound
from dateutil.relativedelta import relativedelta
from discord import AllowedMentions
from discord_slash.utils.manage_commands import create_option
from PIL import Image

from .utils.command_cog import CommandCog
from .utils.commands import command
from .utils.embeds import Embed
from .utils.slash import cog_slash, cog_subcommand
from .utils.utils import ordinal, parse_sql


class Kind(enum.Flag):
    SUBMISSIONS = auto()
    COMMENTS = auto()
    ALL = SUBMISSIONS & COMMENTS


class ModQueueCounter:
    def __init__(self, context, subreddit: Subreddit, kind=Kind.ALL, update_interval=200):
        self.context = context
        self.subreddit = subreddit
        self.kind = kind
        self.update_interval = update_interval

        self.submissions = set()
        self.comments = set()

        self.done = False
        self.errored = False
        self.status = "Starting"

        self._embed: discord.Embed = None
        self.message: discord.Message = None

    @property
    def submission_count(self):
        return len(self.submissions)

    @property
    def comment_count(self):
        return len(self.comments)

    @property
    def total(self):
        return self.submission_count + self.comment_count

    @property
    def embed(self) -> Embed:
        if not self._embed:
            self._embed = Embed(title=f"r/{self.subreddit} Mod Queue Count")
            self._embed.set_footer(text=datetime.now().astimezone().strftime("%B %d, %Y at %I:%M:%S %p %Z"))
        self._embed.color = discord.Color.green() if self.done else discord.Color.orange()
        self._embed.description = self.status if self.done else f"{self.status}..."
        self._embed.clear_fields()
        if self.kind in Kind.SUBMISSIONS:
            self._embed.add_field(name="Posts", value=f"{self.submission_count:,}")
        if self.kind in Kind.COMMENTS:
            self._embed.add_field(name="Comments", value=f"{self.comment_count:,}")
        if self.kind in Kind.ALL:
            self._embed.add_field(name="Total", value=f"{self.total:,}", inline=False)
        return self._embed

    async def _maybe_update_embed(self):
        if self.total % self.update_interval == 0:
            await self.message.edit(embed=self.embed)

    async def start(self):
        try:
            self.status = "Starting"
            self.message = await self.context.channel.send(embed=self.embed)

            if self.kind in Kind.SUBMISSIONS:
                self.status = "Counting Submissions"
                async for item in self.subreddit.mod.modqueue(only="submissions", limit=None):
                    self.submissions.add(item.id)
                    await self._maybe_update_embed()
            if self.kind in Kind.COMMENTS:
                self.status = "Counting Comments"
                async for item in self.subreddit.mod.modqueue(only="comments", limit=None):
                    self.comments.add(item.id)
                    await self._maybe_update_embed()
            self.done = True
            self.status = ""
            await self.context.send(embed=self.embed)
        except Exception as error:
            self.context.bot.log.exception(error)
            self.errored = True
        await self.message.delete()


class RedditStats(CommandCog):
    """
    A collection of Reddit statistic commands
    """

    @cog_slash(
        options=[
            create_option(
                "thing",
                "Can be an ID or url to a post/comment. Required if `user` isn't provided. Can't be used with `user`.",
                str,
                False,
            ),
            create_option(
                "user",
                "A redditor's username. Required if `thing` isn't supplied. Can't be used with `thing`.",
                str,
                False,
            ),
            create_option("mod", "Only get actions made by this moderator.", str, False),
            create_option(
                "limit", "Only fetch this number of actions. Maximum of 10,000 actions. Defaults to 10.", int, False
            ),
            create_option("timezone", "Timezone to display the result in. Defaults to US/Central", str, False),
        ]
    )
    async def action_history(self, context, thing=None, user=None, mod=None, limit=10, timezone="US/Central"):
        """Get recent mod actions performed on a post, comment, or redditor. `thing` or `user` must be provided"""
        await context.defer()
        subreddit = await self.get_subreddit_instance(context)
        reddit = subreddit._reddit
        if not subreddit:
            return
        kwargs = {}
        if thing and user:
            await self.error_embed(context, "`thing` and `user` are mutually exclusive.")
            return
        if not (thing or user):
            await self.error_embed(context, "Either `thing` or `user` is required.")
            return
        if limit > 10000:
            await self.error_embed(
                context,
                "Please specify a limit of ≤10,000 or omit it completely. If you ***need*** more logs, please contact <@393801572858986496>",
                contact_me=False,
            )
            return
        try:
            timezone = pytz.timezone(timezone)
        except pytz.UnknownTimeZoneError:
            await self.error_embed(
                context,
                f"`{timezone}` is not a valid timezone. Please specify a timezone from this [list](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List)",
            )
            return
        if thing:
            comment = None
            submission = None
            if "/" in thing:
                try:
                    comment = await reddit.comment(url=thing)
                except (InvalidURL, NotFound):
                    try:
                        submission = await reddit.submission(url=thing)
                    except (InvalidURL, NotFound):
                        await self.error_embed(context, f"{thing} is not a valid url.")
                        return
            else:
                try:
                    comment = await reddit.comment(thing)
                except (InvalidURL, NotFound):
                    try:
                        submission = await reddit.submission(thing)
                    except (InvalidURL, NotFound):
                        await self.error_embed(context, f"{thing} is not a valid ID.")
                        return
            if comment:
                kwargs["target_id"] = comment.id
                item_kind = "Comment"
                split = comment.permalink.split("/")
                split[5] = "_"
                url = f"https://www.reddit.com{'/'.join(split)}"
            elif submission:
                kwargs["target_id"] = submission.id
                item_kind = "Submission"
                url = submission.shortlink
            else:
                self.log.error("Something went wrong.")
                return
            if (comment or submission).subreddit != subreddit:
                self.log.error(f"That {item_kind.lower()} isn't from this subreddit.")
        else:
            try:
                redditor = await reddit.redditor(user, fetch=True)
                kwargs["target_author"] = redditor.name
                item_kind = "Redditor"
                url = f"https://www.reddit.com/user/{redditor.name}"
            except Exception:
                await self.error_embed(context, f"u/{user} is not a valid user.")
                return
        embed = Embed(title="Fetching Actions")
        embed.add_field(name="Target Kind", value=item_kind)
        embed.add_field(name="Target", value=url)
        if mod:
            try:
                redditor = await reddit.redditor(user, fetch=True)
                kwargs["moderator"] = redditor.name
                embed.add_field(name="Moderator", value=f"u/{redditor.name}")
            except Exception:
                await self.error_embed(context, f"u/{user} is not a valid moderator.")
                return
        embed.description = f"Fetching last {limit:,} actions..."
        message = await context.channel.send(embed=embed)
        await self._get_modlog(context, subreddit, timezone, limit=limit, item_kind=item_kind, **kwargs)
        await message.delete()

    @cog_slash(
        options=[
            create_option(
                "start_date",
                "Matrix start date. Can be human date like `last week`. Defaults to first day of current month.",
                str,
                False,
            ),
            create_option(
                "end_date", "Matrix end date. Can be human date like `last week`. Defaults to today.", str, False
            ),
            create_option("remove_empty_columns", "Remove columns with 0 actions.", bool, False),
            create_option("use_toolbox_method", "Generate the matrix using reddit", bool, False),
        ]
    )
    async def matrix(
        self,
        context,
        start_date: str = None,
        end_date: str = None,
        remove_empty_columns=False,
        use_toolbox_method=False,
    ):
        """Generate a toolbox like mod matrix. Must be used in your sub's channel. If you need help, ask Spaz."""
        await context.defer()
        try:
            start_date, end_date = await self._validate_dates(context, start_date, end_date)
            if start_date and end_date:
                subreddit = await self.get_sub_from_channel(context)
                if not subreddit:
                    return
                embed = await self._gen_date_embed(start_date, end_date)
                message = await context.channel.send(embed=embed)
                if use_toolbox_method:
                    redditor = await self.get_authorized_user(context)
                    if redditor:
                        reddit = self.bot.get_reddit(redditor)
                        sub = await reddit.subreddit(subreddit)
                    else:
                        await self.error_embed(
                            context,
                            "`use_toolbox_method` requires a mod account.",
                        )
                        return
                else:
                    sub = await self.reddit.subreddit(subreddit)
                await self._gen_matrix(
                    context, sub, start_date, end_date, remove_empty_columns, use_toolbox_method, message
                )

        except Exception as error:
            self.log.exception(error)

    @cog_subcommand(
        base="modqueue",
        options=[
            create_option("only", "What to count. Defaults to all.", str, False, choices=["All", "Posts", "Comments"]),
        ],
    )
    async def count(self, context, only="All"):
        """Counts number of items in the modqueue."""
        await context.defer()
        subreddit = await self.get_subreddit_instance(context, "posts")
        if not subreddit:
            return
        kind_mapping = {"All": Kind.ALL, "Posts": Kind.SUBMISSIONS, "Comments": Kind.COMMENTS}
        modqueue_counter = ModQueueCounter(context, subreddit, kind_mapping[only])
        await modqueue_counter.start()
        if modqueue_counter.errored:
            await self.error_embed(context, "Failed to count modqueue.")

    @command(name="modstats", hidden=True, aliases=["ms"])
    async def _modstats(self, context, *args):
        await context.send("This command has been converted into a slash command: `/stats`")

    @cog_slash(
        options=[
            create_option("moderator", "Reddit username. Can not be used with `discord_member`.", str, False),
            create_option(
                "discord_user", "Discord user to check. Can not be used with `moderator`", discord.Member, False
            ),
        ]
    )
    async def stats(self, context, moderator=None, discord_user=None):
        """Get moderator stats for yourself or someone else. Gets your stats if no arguments are provided."""
        await context.defer()
        redditor = None
        error_message = None
        if moderator:
            try:
                redditor = await self.reddit.redditor(moderator)
                redditor = redditor.name
            except asyncprawcore.NotFound:
                error_message = (
                    f"Could not find [u/{moderator}](https://reddit.com/u/{moderator}). Either the account does not exist, has been deleted, or has been suspended.",
                )
        elif discord_user:
            redditor = await self.get_redditor(context, discord_user)
            if not redditor:
                error_message = "That user hasn't verified their account yet."
        else:
            redditor = await self.get_redditor(context)
            if not redditor:
                error_message = "Please use `/stats <user>` or verify your Reddit account with `/verify`."
        if error_message:
            await self.error_embed(context, error_message)
        if redditor:
            await self._calculate_and_send(context, redditor)

    async def _calculate_and_send(self, context, user):
        (
            remaining,
            sub_average,
            sub_count,
            subreddits,
            subscribers,
            zero_count,
        ) = await self.get_and_calculate_subs(user)
        embed = self._gen_embed(user, sub_count, subscribers, sub_average, remaining, zero_count)
        value_string = "\n".join(
            [f"{sub_rank}. {subreddit[0]}: {subreddit[1]:,}" for sub_rank, subreddit in enumerate(subreddits[:20], 1)]
        )
        embed.add_field(name="Top 20 Subreddits", value=value_string, inline=False)
        results = parse_sql(await self.sql.fetch("SELECT * FROM public.moderators WHERE redditor ilike $1", user))
        if results:
            redditor = results[0]
            user = redditor.redditor
            formatted_time = datetime.astimezone(redditor.updated).strftime("%B %d, %Y at %I:%M:%S %p %Z")
            previous_subscriber_count = redditor.subscribers
            previous_sub_count = redditor.subreddits
            embed.set_footer(
                text=f"{sub_count - previous_sub_count:+,} Subreddits and {subscribers - previous_subscriber_count:+,} Subscribers since I last checked on {formatted_time}"
            )
        else:
            embed.set_footer(text=f"{sub_count:+,} Subreddits and {subscribers:+,} Subscribers")
        data = (user, sub_count, subscribers)
        results = parse_sql(await self.sql.fetch("SELECT * FROM public.moderators WHERE redditor=$1", user))
        if results:
            await self.sql.execute(
                "UPDATE public.moderators SET subreddits=$2, subscribers=$3 WHERE redditor=$1",
                *data,
            )
        else:
            await self.sql.execute(
                "INSERT INTO public.moderators(redditor, subreddits, subscribers) VALUES ($1, $2, $3)",
                *data,
            )
        await context.send(embed=embed)

    def _check_date(self, date, *, last_month=False, current_month=False, today=False):
        parsed_date = None
        if date:
            if isinstance(date, int):
                if 1 <= date <= 12:
                    parsed_date = self.parse_date(date)
            else:
                parsed_date = self.parse_date(date)
        else:
            if today:
                parsed_date = self.parse_date(time.strftime("%m/%d/%Y %I:%M:%S %p", time.gmtime()))
            else:
                if last_month:
                    months = 1
                if current_month:
                    months = 0
                parsed_date = self.parse_date(
                    time.strftime(
                        "%m",
                        time.gmtime(datetime.timestamp(datetime.today() - relativedelta(months=months))),
                    )
                )
        return parsed_date

    @staticmethod
    async def _gen_date_embed(start_date, end_date):
        embed = Embed(title="Generating Matrix", color=discord.Color.blurple())
        embed.add_field(
            name="Starting Date",
            value=start_date.strftime(f"%B {ordinal(start_date.day)}, %Y"),
        )
        embed.add_field(
            name="Ending Date",
            value=end_date.strftime(f"%B {ordinal(end_date.day)}, %Y"),
        )
        return embed

    @staticmethod
    def _gen_embed(user, sub_count, subscribers, sub_average, remaining, zero_count):
        embed = discord.Embed(
            title=f"Moderated Subreddit Stats for u/{user}",
            url=f"https://www.reddit.com/user/{user}",
        )
        embed.add_field(name="Reddit Username", value=user)
        embed.add_field(name="Subreddit Count", value=f"{sub_count:,}")
        embed.add_field(name="Subscriber Count", value=f"{subscribers:,}")
        embed.add_field(name="Avg. Subscriber Count", value=f"{sub_average:,}")
        embed.add_field(name="Subreddits with 1 Subs", value=f"{remaining:,}")
        embed.add_field(name="Subreddits with 0 Subs", value=f"{zero_count:,}")
        return embed

    async def _gen_matrix(self, context, subreddit, start_date, end_date, remove_empty_columns, tb=False, message=None):
        try:
            async with self.sql.acquire() as sql:
                if tb:
                    thingTypes = {
                        "t1": "Comment",
                        "t2": "Account",
                        "t3": "Link",
                        "t4": "Message",
                        "t5": "Subreddit",
                        "t6": "Award",
                    }
                    start_epoch = start_date.timestamp()
                    end_epoch = end_date.timestamp()
                    # noinspection PyTypeChecker
                    Result = NamedTuple(
                        "Result",
                        [
                            ("moderator", str),
                            ("mod_action", str),
                            ("target_type", str),
                        ],
                    )
                    results = []
                    i = 0
                    async for action in subreddit.mod.log(limit=None):
                        if start_epoch <= action.created_utc <= end_epoch:
                            i += 1
                            if i == 1 or i % 1000 == 0 and i != 0:
                                fields = (
                                    {
                                        "name": "Subreddit",
                                        "value": subreddit.display_name,
                                    },
                                    {
                                        "name": "Starting Date",
                                        "value": start_date.strftime(f"%B {ordinal(start_date.day)}, %Y"),
                                    },
                                    {
                                        "name": "Ending Date",
                                        "value": end_date.strftime(f"%B {ordinal(end_date.day)}, %Y"),
                                    },
                                    {"name": "Counted Actions", "value": f"{i:,}"},
                                    {
                                        "name": "Current Action Date",
                                        "value": time.strftime(
                                            "%m/%d/%Y %I:%M:%S %p",
                                            time.localtime(action.created_utc),
                                        ),
                                    },
                                )
                                message = await self.status_update_embed(
                                    message, "Getting mod actions from reddit...", *fields
                                )
                            thingType = None
                            if action.target_fullname:
                                thingType = thingTypes[action.target_fullname.split("_")[0]]
                            logAction = Result(
                                moderator=action._mod,
                                mod_action=action.action,
                                target_type=thingType,
                            )
                            results.append(logAction)
                        elif action.created_utc > end_epoch:
                            continue
                        else:
                            message.embeds[0].color = discord.Color.green()
                            message = await self.status_done_embed(message, "Done", *fields)
                            break
                else:
                    data = (subreddit.display_name, start_date, end_date)
                    query = await asyncpg.utils._mogrify(
                        sql,
                        "SELECT moderator, mod_action FROM mirror.modlog WHERE subreddit=$1 and created_utc > $2 and created_utc < $3;",
                        data,
                    )
                    self.log.debug(query)
                    results = await sql.fetch(
                        "SELECT moderator, mod_action, target_type FROM mirror.modlog WHERE subreddit=$1 and created_utc > $2 and created_utc < $3;",
                        *data,
                        timeout=10000,
                    )
                    results = parse_sql(results)
                action_types = set()
                action_types = self._simplify_mod_actions(action_types, results)
                mods = {result.moderator: {action: 0 for action in action_types} for result in results}
                subMods = await subreddit.moderator()
                for mod in subMods:
                    mods[mod.name] = {action: 0 for action in action_types}
                for result in results:
                    mods[result.moderator][self._simplify_action(result)] += 1
                df = pandas.DataFrame(mods)
                df.loc["Total"] = df.sum()
                df = df.transpose()
                df = df.sort_values("Total", ascending=False)
                df.loc["Total"] = df.sum()
                df = df.transpose()
                df = df.drop("Total")
                df = df.sort_values("Total", ascending=False)
                df.loc["Total"] = df.sum()
                df = df.transpose()
                total_column = df.pop("Total")
                df.insert(0, "Total", total_column)
                if remove_empty_columns:
                    df = df.loc[:, (df != 0).any(axis=0)]
                filename = f'{subreddit.display_name}-matrix-{start_date.strftime("%m/%d/%Y")}-to-{end_date.strftime("%m/%d/%Y")}'
                matrix = io.BytesIO()
                dataframe_image.export(df, matrix, max_cols=-1, max_rows=-1, table_conversion="matplotlib")
                matrix = io.BytesIO(matrix.getvalue())
                image = await self.bot.file_storage.send(file=discord.File(matrix, filename=f"{filename}.png"))
                csv_file = await self.bot.file_storage.send(
                    file=discord.File(io.BytesIO(df.to_csv().encode()), filename=f"{filename}.csv")
                )
                embed = Embed(
                    title="Matrix", description=f'{start_date.strftime("%m/%d/%Y")} to {end_date.strftime("%m/%d/%Y")}'
                )
                embed.add_field(name="Download file", value=f"[{filename}.csv]({csv_file.attachments[0].url})")
                embed.set_image(url=image.attachments[0].url)
                if message:
                    await message.delete()
                await context.send(
                    f"Hey {context.author.mention}, here is your mod matrix:",
                    embed=embed,
                    allowed_mentions=AllowedMentions.all(),
                )
        except CancelledError:
            await self.cancelled_embed(context, "Matrix generation was cancelled.")
        except Exception as error:
            self.log.exception(error)

    async def _get_modlog(
        self,
        context,
        subreddit,
        timezone,
        target_id=None,
        target_author=None,
        moderator=None,
        limit=10,
        item_kind=None,
    ):
        try:
            self.log.info("Getting Logs")
            sql_str = (
                "SELECT created_utc, moderator, mod_action, details, description FROM mirror.modlog WHERE subreddit=$1"
            )
            parts = [sql_str]
            data = [str(subreddit)]
            names = [str(subreddit)]
            suffix = "action_history"

            def next_arg(column, value):
                parts.append("AND")
                parts.append(f"{column}=${len(data)+1}")
                data.append(value)

            if target_id:
                next_arg("target_id", target_id)
                names.append(target_id)
            else:
                next_arg("target_author", target_author)
                names.append(target_author)
            if moderator:
                next_arg("moderator", moderator)
                names.append(moderator)
            parts.append(f"ORDER BY created_utc DESC LIMIT ${len(data)+1}")
            data.append(limit)

            results = parse_sql(await self.sql.fetch(" ".join(parts), *data))
            if results:
                header = {
                    "index": "",
                    "moderator": "Moderator",
                    "mod_action": "Action",
                    "created_utc": timezone.normalize(datetime.utcnow().astimezone(pytz.utc)).strftime(
                        "Timestamp (%Z)"
                    ),
                    "details": "Details",
                    "description": "Description",
                }
                rows = []
                for index, action in enumerate(results, 1):
                    row = [index]
                    for column in list(header.keys())[1::]:
                        if column == "created_utc":
                            row.append(
                                timezone.normalize(action.created_utc.astimezone(pytz.utc)).strftime(
                                    "%m-%d-%Y %H:%M:%S"
                                )
                            )
                        elif column == "description":
                            value = getattr(action, column)
                            if value:
                                row.append(value)
                            else:
                                del header[column]
                        else:
                            row.append(getattr(action, column))
                    rows.append(row)
                mod_str = f" by u/{moderator}" if moderator else ""
                if target_author:
                    target_str = f"u/{target_author}"
                else:
                    target_str = target_id

                embed = Embed(
                    title="Action History",
                    description=f"Last {limit:,} actions on {item_kind.lower()} {target_str}{mod_str}",
                )

                df = pandas.DataFrame(rows, columns=header.values())
                df.set_index("", inplace=True)
                filename = "_".join(names + [suffix])
                if len(rows) <= 40:
                    th_props = [
                        ("font-size", "11px"),
                        ("text-align", "center"),
                        ("font-weight", "bold"),
                        ("color", "#dcddde"),
                        ("background-color", "#202225"),
                    ]

                    # Set CSS properties for td elements in dataframe
                    td_props = [
                        ("font-size", "11px"),
                        ("text-align", "right"),
                        ("color", "#dcddde"),
                        ("background-color", "#202225"),
                    ]

                    # Set table styles
                    styles = [dict(selector="th", props=th_props), dict(selector="td", props=td_props)]

                    styled = df.style.set_table_styles(styles)
                    uncropped = io.BytesIO()
                    dataframe_image.export(styled, uncropped, max_cols=-1, table_conversion="matplotlib")
                    image = Image.open(uncropped)
                    width, height = image.size
                    matrix = io.BytesIO()
                    image.crop((1, 1, width - 1, height - 1)).save(matrix, "png")
                    matrix.seek(0)
                    image = await self.bot.file_storage.send(file=discord.File(matrix, filename=f"{filename}.png"))
                    embed.set_image(url=image.attachments[0].url)
                csv_file = await self.bot.file_storage.send(
                    file=discord.File(io.BytesIO(df.to_csv().encode()), filename=f"{filename}.csv")
                )
                embed.add_field(name="Download file", value=f"[{filename}.csv]({csv_file.attachments[0].url})")
                await context.send(embed=embed)
        except Exception as error:
            self.log.exception(error)

    @staticmethod
    def parse_date(date=None):
        settings = dateparser.conf.Settings()
        settings.PREFER_DAY_OF_MONTH = "first"
        settings.RETURN_AS_TIMEZONE_AWARE = True
        settings.TIMEZONE = "UTC"
        settings.PREFER_DATES_FROM = "past"
        try:
            parsed_date = dateparser.parse(date)
            return parsed_date
        except Exception:
            return

    @staticmethod
    def _simplify_action(result):
        mod_action = result.mod_action
        if "sticky" in mod_action or "lock" in mod_action:
            mod_action += {"submission": "link", "link": "link", "comment": "comment"}[result.target_type.lower()]
        return mod_action

    def _simplify_mod_actions(self, action_types, results):
        for result in results:
            mod_action = self._simplify_action(result)
            action_types.add(mod_action)
        return action_types

    async def _validate_dates(self, context, starting_date, ending_date):
        start_date = self._check_date(starting_date, current_month=True)
        end_date = self._check_date(ending_date, today=True)
        invalid_date = None
        if not start_date:
            invalid_date = starting_date
        if not end_date:
            invalid_date = ending_date
        if invalid_date:
            await context.send(
                embed=self.generate_error_embed(
                    f"`{invalid_date}` is not a valid date, please use a number between 1 and 12 or a valid date."
                )
            )
            return None, None
        if end_date < start_date:
            await context.send(embed=self.generate_error_embed("Start date must be before end date"))
            return None, None
        return start_date, end_date


def setup(bot):
    bot.add_cog(RedditStats(bot))
