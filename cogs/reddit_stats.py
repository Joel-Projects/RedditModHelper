"""Commands for getting various Reddit Moderation statistics"""
import datetime as dt
import hashlib
import io
import os
import re
import textwrap
import time
from asyncio import CancelledError
from datetime import datetime
from typing import NamedTuple

import asyncpg
import asyncpraw
import asyncprawcore
import dataframe_image
import dateparser
import discord
import pandas
import pytz
import urllib3
from dateutil.relativedelta import relativedelta
from discord import Embed
from discord_slash.utils.manage_commands import create_option

from .utils.command_cog import CommandCog
from .utils.commands import command
from .utils.context import Context
from .utils.slash import cog_slash
from .utils.utils import gen_date_string, ordinal, parse_sql


def parse_date(date=None):
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


class RedditStats(CommandCog):
    """
    A collection of Reddit statistic commands
    """

    @command(aliases=["ml"])
    async def modlog(
        self,
        context: Context,
        *args,
        url: str = None,
        mod: str = None,
        sub=None,
        limit=None,
        timezone: str = "Eastern",
    ):
        """
        Get mod actions from <url> or <user> by optional <mod>, or from <subreddit> by optional <mod>, or by <mod>

        Parameters:
            url: Reddit link for a submission, comment or redditor
            redditor: Redditor to check
            user: Alias for redditor
            moderator: Name of moderator
            mod: Alias for moderator
            subreddit: Name of subreddit you want logs from
            sub: Alias for subreddit
            limit: Limits how many mod actions are returned (default: all)

        Examples:
         ```.modlog url=https://www.reddit.com/u/lil_spazjoekp mod=siouxsie_siouxv2``` will return all actions made by u/siouxsie_siouxv2 on user u/lil_spazjoekp
         ```.modlog url=https://www.reddit.com/u/lil_spazjoekp``` will return all actions made on user u/lil_spazjoekp
         ```.modlog mod=lil_spazjoekp limit=100``` will return 100 of the most recent actions made by u/lil_spazjoekp on all moderated subreddits
         ```.modlog user=quimpers``` will return all actions made on u/quimpers in all moderated subreddits
         ```.modlog user=quimpers mod=lil_spazjoekp``` will return all actions made by u/lil_spazjoekp on u/quimpers in all moderated subreddits
         ```.modlog user=quimpers mod=lil_spazjoekp sub=fakehistoryporn``` will return all actions made by u/lil_spazjoekp on u/quimpers in r/fakehistoryporn
         ```.modlog sub=fakehistoryporn``` will return all actions made in r/fakehistoryporn
         ```.modlog sub=fakehistoryporn limit=100``` will return 100 of the most recent actions made in r/fakehistoryporn
            More coming soon...
        """

        kwargs = await self.parseKwargs(args, context)

        self.bot.start_task(context, self.getModlog(context, **kwargs))

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
        """Parameters:
            start_date: Start date to get actions from. If not specified first day of current month is used. If the date has a space `"` or `'` are needed.
            end_date: End date to get actions from. If not specified current day is used. If the date has a space `"` or `'` are needed.

        Note: you can use a wide variety of date formats (e.g., today, yesterday, may, 1/1/2019 1/1/19, "mar 1", etc.) If it has a space in it, it must be quoted.

        Examples: (Note this will have the dates as if you ran it right now)

         To generate a matrix for r/pics between:
          **`{none}`**
          to
          **`{today}`**```.matrix pics```
         To generate a matrix for r/pics between:
          **`{may}`**
          to
          **`{today}`**```.matrix pics may```
         To generate a matrix for r/pics between:
          **`{may_21}`**
          to
          **`{today}`**```.matrix pics "may 21"```
         To generate a matrix for r/pics between:
          **`{last_week}`**
          to
          **`{today}`**```.matrix pics "last week"```
         To generate a matrix for r/pics between:
          **`{april}`**
          to
          **`{may}`**```.matrix pics april may```
         To generate a matrix for r/pics between:
          **`{"1"}`**
          to
          **`{today}`**```.matrix pics 1```
         To generate a matrix for r/pics between:
          **`{1_22}`**
          to
          **`{today}`**```.matrix pics 1/22```
         To generate a matrix for r/pics between:
          **`{12_22_18}`**
          to
          **`{1_22_19}`**```.matrix pics 12/22/18 1/22/2019```
         To generate a matrix for r/pics between:
          **`{5_days_ago}`**
          to
          **`{today}`**```.matrix pics "5 days ago"```
        If you are in a sub channel, the subreddit does not need to be specified.
        If you have any questions ask Spaz.
        """
        await context.defer()
        try:
            start_date, end_date = await self.validate_dates(context, start_date, end_date)
            if start_date and end_date:
                subreddit = await self.get_sub_from_channel(context)
                if subreddit:
                    embed = await self.generate_date_embed(start_date, end_date)
                    message = await context.channel.send(embed=embed)
                    if use_toolbox_method:
                        redditor = await self.get_authorized_user(context)
                        if redditor:
                            reddit = self.bot.get_reddit(redditor)
                            sub = await reddit.subreddit(subreddit)
                        else:
                            await self.error_embed(
                                context,
                                "`use_toolbox_method` requires a mod account. If you need more help, contact <@393801572858986496>.",
                            )
                            return
                    else:
                        sub = await self.reddit.subreddit(subreddit)
                    await self.gen_matrix(
                        context, sub, start_date, end_date, remove_empty_columns, use_toolbox_method, message
                    )
                else:
                    await self.error_embed(context, "This command can only be used in a sub channel.")
                    return
        except Exception as error:
            self.log.exception(error)

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
            await self.calculate_and_send(context, redditor)

    @command()
    async def traffic(self, context, *subreddits: str):
        """
        Gets traffic for [subreddit(s)]

        Parameters:
            subreddit(s): a single subreddit or subreddits separated by spaces

        Examples:
         ```.traffic``` in sub channel
         ```.traffic dankmemes```
         ```.traffic dankmemes memes```
        """
        async with context.typing():
            if subreddits:
                for subreddit in subreddits:
                    await self.parseTraffic(subreddit, context)
            else:
                subreddit = await self.get_sub_from_channel(context)
                if subreddit:
                    await self.parseTraffic(subreddit, context)
                else:
                    await self.error_embed(
                        context,
                        "Please use this command in a sub channel or specify sub.",
                    )

    @command(aliases=["lma"])
    async def lastmodaction(self, context, *moderators):
        """
        Gets last mod action for yourself or `moderator(s)`

        Parameters:
            moderators: a single moderator or moderators separated by spaces

        Examples:
         ```.lastmodaction``` for yourself
         ```.lastmodaction Lil_SpazJoekp```
         ```.lastmodaction Lil_SpazJoekp siouxsie_siouxv2```
        """
        async with context.typing():
            # globalMods, subs, multis = await self.parseLmaKwargs(moderators)
            if not moderators:
                author = context.message.author
                userID = author.id
                encodedAuthor = hashlib.sha256(str(userID).encode("utf-8")).hexdigest()
                results = parse_sql(await self.sql.fetch("SELECT * FROM verified WHERE id=$1", encodedAuthor))
                if results:
                    moderators = [results[0].redditor]
                else:
                    await self.error_embed(
                        context,
                        "Please a moderator or verify your Reddit account with `.verify`.",
                    )
            for moderator in moderators:
                mod = await self.get_mod(context, moderator)
                if mod:
                    results = parse_sql(
                        await self.sql.fetch(
                            "SELECT * FROM mirror.modlog WHERE moderator=$1 ORDER BY created_utc DESC LIMIT 1",
                            mod,
                        )
                    )
                    if results:
                        lastAction = results[0]
                        embed = self.gen_action_embed(lastAction)
                        await context.send(embed=embed)
                    else:
                        await self.error_embed(
                            context,
                            f"Could not find any action performed by [u/{mod}](https://reddit.com/user/{mod})",
                        )

    async def parseLmaKwargs(self, context, args):
        """

        :param args:
        :return:
        """
        kwargs = {}
        validKwargs = ["sub", "subreddit", "action", "multi", "multireddit"]
        kwargMapping = {
            "sub": "sub",
            "subreddit": "sub",
            "action": "action",
            "multi": "multi",
            "multireddit": "multi",
        }
        validKwargStr = ", ".join(
            [arg if not validKwargs.index(arg) + 1 == len(validKwargs) else f"and {arg}" for arg in validKwargs]
        )
        subGroups = {}
        multiGroups = {}
        regex = r"((sub|subreddit|multi|multireddit)=(.*?( |$)))"
        argStr = " ".join(args)
        mods = []
        groups = []
        for arg in args:
            if any([i in arg for i in validKwargs]):
                if arg == "multi" or arg == "multireddit":
                    mods = []
                # mods = []
            else:
                mods.append(arg)

        for arg in args:
            splitArg = arg.split("=")
            splitArg.reverse()
            kwarg = splitArg.pop()
            splitArg.reverse()
            if kwarg in validKwargs:
                kwargs[kwargMapping[kwarg]] = "".join(splitArg)
            else:
                # print(f"{kwarg} isn't a valid argument. Valid arguments are: {validKwargStr}\nSkipping...")
                await self.error_embed(
                    context,
                    f"{kwarg} isn't a valid argument. Valid arguments are: {validKwargStr}\nSkipping...",
                )
        return kwargs

    async def parseTraffic(self, subreddit, context):
        daystr = "Date,Uniques,Pageviews,Subscriptions"
        hourstr = "Date,Uniques,Pageviews"
        monthstr = "Date,Uniques,Pageviews"
        subResults = await self.checkSub(context, subreddit)
        if subResults:
            subreddit = subResults["subreddit"]
            sub = await self.reddit.subreddit(subreddit)
            subreddit = sub.display_name
            if subResults["isMod"]:
                traffic = await sub.traffic()
                for day in traffic["day"]:
                    date = gen_date_string(day[0], True, "%m/%d/%Y")
                    daystr += f"\n{date},{day[1]},{day[2]},{day[3]}"
                with open(f"Daily Traffic Stats for {subreddit}.csv", "w") as csv:
                    csv.write(daystr)

                for hour in traffic["hour"]:
                    date = gen_date_string(hour[0], True, "%m/%d/%Y %I %p %Z")
                    hourstr += f"\n{date},{hour[1]},{hour[2]}"
                with open(f"Hourly Traffic Stats for {subreddit}.csv", "w") as csv:
                    csv.write(hourstr)

                for month in traffic["month"]:
                    date = gen_date_string(month[0], True, "%B %Y")
                    monthstr += f"\n{date},{month[1]},{month[2]}"
                with open(f"Monthly Traffic Stats for {subreddit}.csv", "w") as csv:
                    csv.write(monthstr)
                files = [
                    discord.File(f"Daily Traffic Stats for {subreddit}.csv"),
                    discord.File(f"Hourly Traffic Stats for {subreddit}.csv"),
                    discord.File(f"Monthly Traffic Stats for {subreddit}.csv"),
                ]
                await context.send(files=files)
                for file in files:
                    os.remove(file.fp)
            else:
                await self.error_embed(context, f"You don't moderate r/{subreddit}")

    async def parseKwargs(self, args, context):
        kwargs = {}
        validKwargs = [
            "url",
            "user",
            "redditor",
            "mod",
            "moderator",
            "sub",
            "subreddit",
            "limit",
            "timezone",
            "action",
        ]
        kwargMapping = {
            "url": "url",
            "mod": "mod",
            "user": "user",
            "redditor": "user",
            "sub": "sub",
            "limit": "limit",
            "timezone": "timezone",
            "moderator": "mod",
            "subreddit": "sub",
            "action": "action",
        }
        validKwargStr = ", ".join(
            [arg if not validKwargs.index(arg) + 1 == len(validKwargs) else f"and {arg}" for arg in validKwargs]
        )
        for arg in args:
            splitArg = arg.split("=")
            splitArg.reverse()
            kwarg = splitArg.pop()
            splitArg.reverse()
            if kwarg in validKwargs:
                if kwarg == "action":
                    if splitArg[0] in actionMapping:
                        kwargs[kwargMapping[kwarg]] = "".join(splitArg)
                    else:
                        await self.error_embed(
                            context,
                            f"{splitArg} is not a valid action. Do `.validactions` to see valid actions.\nSkipping...",
                        )
                else:
                    kwargs[kwargMapping[kwarg]] = "".join(splitArg)
            else:
                await self.error_embed(
                    context,
                    f"{kwarg} isn't a valid argument. Valid arguments are: {validKwargStr}\nSkipping...",
                )
        return kwargs

    async def checkUrl(self, context, url: str):
        self.log.info("Checking url")
        reddit = self.reddit
        userRegex = r"https?:\/\/(www\.|old\.|new\.)?reddit\.com\/(user|u)/.*?$"
        match = re.search(userRegex, url, re.MULTILINE)
        if match:
            user = urllib3.util.parse_url(match[0]).path
            if user[-1] == "/":
                user = user[:-1].split("/")[-1]
            else:
                user = user.split("/")[-1]
            try:
                redditor = await reddit.redditor(user)
                return redditor
            except asyncprawcore.NotFound:
                await self.error_embed(context, f"Could not find u/{user}")
        else:
            urlRegex = r"https?:\/\/(((www\.|old\.|new\.)?reddit\.com)|redd\.it)\/"
            match = re.search(urlRegex, url, re.MULTILINE)
            if match:
                try:
                    thing = reddit.comment(url=url)
                    return thing
                except asyncpraw.exceptions.ClientException:
                    try:
                        thing = reddit.submission(url=url)
                        return thing
                    except Exception as error:
                        self.log.error(error)
                        self.log.info(error)
                        await self.error_embed(context, "Hmm..that url didn't work")
                except Exception as error:
                    self.log.error(error)
                    self.log.info(error)
                    await self.error_embed(context, "Hmm..that url didn't work")

    async def getModlog(
        self,
        context,
        url: str = None,
        mod: str = None,
        user: str = None,
        sub=None,
        limit: int = None,
        timezone: str = "Eastern",
        action=None,
    ):
        """

        :param url:
        :param mod:
        :param user:
        :param sub:
        :param limit:
        :param timezone:
        """
        try:
            async with self.bot.pool.acquire() as sql:
                async with context.typing():
                    self.log.info("Getting Logs")
                    sqlstr = "SELECT created_utc, moderator, subreddit, mod_action, details, description, target_author, target_body, target_type, target_id, target_permalink, target_title FROM mirror.modlog"
                    subreddit = None
                    global argIndex
                    argIndex = 1
                    parts = [sqlstr]
                    values = []
                    names = []
                    suffix = "modlog.csv"

                    def nextArg(arg, value):
                        global argIndex
                        nextIndex = lambda i: parts.append(f"{i}=${argIndex}")
                        if len(parts) > 1:
                            parts.append("AND")
                        else:
                            parts.append("WHERE")
                        nextIndex(arg)
                        values.append(value)
                        argIndex += 1

                    try:
                        if limit:
                            limit = int(limit)
                    except:
                        await self.error_embed(context, "Limit must be a whole number, skipping limit")
                    if user or url or mod or sub or action:
                        if user:
                            userRegex = r"https?:\/\/(www\.|old\.|new\.)?reddit\.com\/(user|u)/.*?$"
                            match = re.search(userRegex, user, re.MULTILINE)
                            if match:
                                thing = await self.checkUrl(context, user)
                            else:
                                thing = await self.checkUrl(context, f"https://reddit.com/u/{user}")
                            if thing:
                                nextArg("target_author", thing.name)
                                names.append(thing.name)
                        elif url:
                            thing = await self.checkUrl(context, url)
                            if thing:
                                if isinstance(thing, asyncpraw.models.Redditor):
                                    nextArg("target_author", thing.name)
                                    names.append(thing.name)
                                else:
                                    nextArg("target_id", thing.id)
                                    names.append(thing.id)
                        if mod:
                            modname = await self.getmod(context, mod)
                            nextArg("moderator", modname)
                            names.append(modname)
                        if sub:
                            nextArg("subreddit", sub)
                            names.append(sub)
                        if action:
                            nextArg("mod_action", action)
                            names.append(action)
                        parts.append("ORDER BY created_utc DESC")
                        if limit:
                            parts.append(f"LIMIT ${argIndex}")
                            values.append(limit)
                        sqlStatement = await sql.prepare((" ".join(parts)))
                        filename = "_".join(names + [suffix])
                        link = await self.generateCSV(context, sqlStatement, filename, timezone, tuple(values))
                        await context.send(
                            f"Hey {context.author.mention}, got your requested modlogs logs. You can view it here: {link}"
                        )
                    else:
                        await self.error_embed(
                            context,
                            "Please include at least one of the following:```url, mod, user, limit, action, or sub```or run this command in a sub specific channel",
                        )
        except CancelledError:
            pass

    async def gen_matrix(self, context, subreddit, start_date, end_date, remove_empty_columns, tb=False, message=None):
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
                                fields = ({
                                              "name": "Subreddit", "value": subreddit.display_name,
                                          }, {
                                              "name": "Starting Date", "value": start_date.strftime(f"%B {ordinal(start_date.day)}, %Y"),
                                          }, {
                                              "name": "Ending Date", "value": end_date.strftime(f"%B {ordinal(end_date.day)}, %Y"),
                                          }, {"name": "Counted Actions", "value": f"{i:,}"}, {
                                              "name": "Current Action Date", "value": time.strftime("%m/%d/%Y %I:%M:%S %p", time.localtime(action.created_utc), ),
                                          },)
                                message = await self.status_update_embed(message, "Getting mod actions from reddit...", *fields)
                            thingType = None
                            if action.target_fullname:
                                thingType = thingTypes[action.target_fullname.split("_")[0]]
                            logAction = Result(
                                moderator=action._mod,
                                mod_action=action.action,
                                target_type=thingType,
                            )
                            results.append(logAction)
                            self.log.info(f"{i:,} :: {logAction}")
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
                action_types = self._simply_mod_actions(action_types, results)
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
                filename = (
                    f'{subreddit.display_name}-matrix-{start_date.strftime("%m/%d/%Y")}-to-{end_date.strftime("%m/%d/%Y")}'
                )
                matrix = io.BytesIO()
                dataframe_image.export(df, matrix, max_cols=-1, table_conversion="matplotlib")
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
                await context.send(f"Hey {context.author.mention}, here is your mod matrix:", embed=embed)
        except CancelledError:
            await self.cancelled_embed(context, 'Matrix generation was cancelled.')
        except Exception as error:
            self.log.exception(error)

    async def validate_dates(self, context, starting_date, ending_date):
        start_date = await self.check_date(starting_date, current_month=True)
        end_date = await self.check_date(ending_date, today=True)
        invalid_date = None
        if not start_date:
            invalid_date = starting_date
        if not end_date:
            invalid_date = ending_date
        if invalid_date:
            await context.send(embed=self.generate_error_embed(
                    f"`{invalid_date}` is not a valid date, please use a number between 1 and 12 or a valid date."
                )
            )
            return None, None
        if end_date < start_date:
            await context.send(embed=self.generate_error_embed("Start date must be before end date"))
            return None, None
        return start_date, end_date

    @staticmethod
    async def generate_date_embed(start_date, end_date):
        embed = Embed(title="Generating Matrix")
        embed.add_field(
            name="Starting Date",
            value=start_date.strftime(f"%B {ordinal(start_date.day)}, %Y"),
        )
        embed.add_field(
            name="Ending Date",
            value=end_date.strftime(f"%B {ordinal(end_date.day)}, %Y"),
        )
        return embed

    def _simply_mod_actions(self, action_types, results):
        for result in results:
            mod_action = self._simplify_action(result)
            action_types.add(mod_action)
        return action_types

    @staticmethod
    def _simplify_action(result):
        mod_action = result.mod_action
        if "sticky" in mod_action or "lock" in mod_action:
            mod_action += {"submission": "link", "link": "link", "comment": "comment"}[result.target_type.lower()]
        return mod_action

    async def check_date(self, date, *, last_month=False, current_month=False, today=False):
        parsed_date = None
        if date:
            if isinstance(date, int):
                if 1 <= date <= 12:
                    parsed_date = parse_date(date)
            else:
                parsed_date = parse_date(date)
        else:
            if today:
                parsed_date = parse_date(time.strftime("%m/%d/%Y %I:%M:%S %p", time.gmtime()))
            else:
                if last_month:
                    months = 1
                if current_month:
                    months = 0
                parsed_date = parse_date(
                    time.strftime(
                        "%m",
                        time.gmtime(datetime.timestamp(datetime.today() - relativedelta(months=months))),
                    )
                )
        return parsed_date

    def gen_embed(self, user, sub_count, subscribers, sub_average, remaining, zero_count):
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

    async def calculate_and_send(self, context, user):
        (
            remaining,
            sub_average,
            sub_count,
            subreddits,
            subscribers,
            zero_count,
        ) = await self.get_and_calculate_subs(user)
        embed = self.gen_embed(user, sub_count, subscribers, sub_average, remaining, zero_count)
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

    def gen_action_embed(self, action):
        embed = Embed(
            title="Most Recent Mod Action",
            description=f"Last action performed by [u/{action.moderator}](https://reddit.com/user/{action.moderator})",
        )
        embed.add_field(name="Action", value=actionMapping[action.mod_action])
        embed.add_field(
            name="Action Date/Time",
            value=action.created_utc.astimezone(pytz.timezone("US/Pacific")).strftime("%B %d, %Y at %I:%M:%S %p %Z"),
        )
        if getattr(action, "subreddit", None):
            subreddit = f"[{action.subreddit}](https://reddit.com/r/{action.subreddit})"
        else:
            subreddit = "None"
        embed.add_field(name="Subreddit", value=subreddit)
        embed.add_field(name="Details", value=getattr(action, "details", "None"))
        embed.add_field(name="Description", value=getattr(action, "description", "None"))
        if getattr(action, "target_title", None) and getattr(action, "target_permalink", None):
            embed.add_field(
                name="Target",
                value=f"[{action.target_title}](https://reddit.com{action.target_permalink})",
            )
        else:
            embed.add_field(name="Target", value="None")
        if getattr(action, "target_author", None) and getattr(action, "target_author", None) != "":
            if getattr(action, "target_author", None):
                targetAuthor = f"[{action.target_author}](https://reddit.com/user/{action.target_author})"
            else:
                targetAuthor = "None"
            embed.add_field(name="Target Author", value=targetAuthor)

        if action.target_body:
            bodySections = textwrap.wrap(action.target_body, 1021)
            if len(bodySections) == 1:
                embed.add_field(name="Target Body", value=f"{bodySections[0]}")
            else:
                embed.add_field(name="Target Body", value=f"{bodySections[0]}...")
        score = None
        self.log.debug(embed.to_dict())
        return embed


def setup(bot):
    bot.add_cog(RedditStats(bot))
