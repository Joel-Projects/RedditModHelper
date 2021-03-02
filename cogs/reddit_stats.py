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
import dateutil
import discord
import pandas
import pytz
import urllib3
from discord import Embed

from .utils import db
from .utils.command_cog import CommandCog
from .utils.commands import command
from .utils.context import Context
from .utils.utils import genDateString, ordinal, parse_sql


def parseDate(date: str = None):

    settings = dateparser.conf.Settings()
    settings.PREFER_DAY_OF_MONTH = 'first'
    settings.RETURN_AS_TIMEZONE_AWARE = True
    settings.TIMEZONE = 'UTC'
    settings.PREFER_DATES_FROM = 'past'
    try:
        parsedDate = dateparser.parse(date)
        return parsedDate
    except Exception:
        return


class Subreddits(db.Table, table_name="subreddits"):
    name = db.Column(db.String, primary_key=True, unique=True, nullable=False)
    mod_role = db.Column(db.Integer(big=True), nullable=False)
    channel_id = db.Column(db.Integer(big=True), nullable=False)
    modlog_account = db.Column(db.String, nullable=False)
    alert_channel_id = db.Column(db.Integer(big=True))


class RedditStats(CommandCog):
    '''
    A collection of Reddit statistic commands
    '''

    def __init__(self, bot):
        super().__init__(bot)
        self.checkDateErrored = False

    @command(aliases=['ml'])
    async def modlog(self, context: Context, *args, url: str = None, mod: str = None, sub=None, limit=None, timezone: str = 'Eastern'):
        '''
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
        '''

        kwargs = await self.parseKwargs(args, context)

        self.bot.start_task(context, self.getModlog(context, **kwargs))

    @command(aliases=['mx'])
    async def matrix(self, context: Context, *args, startDate: str = None, endDate: str = None):
        '''
        Generate a toolbox like mod matrix for <subreddit> from <startDate> [optional] to <endDate> [optional]

        Parameters:
            startDate: Start date to get actions from. If not specified first day of current month is used. If the date has a space `"` or `'` are needed.
            endDate: End date to get actions from. If not specified current day is used. If the date has a space `"` or `'` are needed.

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
        '''
        subreddit = await self.getSubFromChannel(context)
        authorized_user = await self.getAuthorizedUser(context)
        if len(args) == 2:
            startDate, endDate = args[:2]
        elif len(args) == 1:
            startDate = args[0]
        if subreddit:
            try:
                sub = await self.reddit.subreddit(subreddit)
                self.bot.start_task(context, self.genMatrix(context, sub, startDate, endDate))
            except asyncprawcore.exceptions.NotFound:
                await self.error_embed(context, f'r/pics was not found.')
                return
        else:
            await self.error_embed(context, 'This command can only be used in a sub channel.')
            return

    @command(aliases=['tbmx'])
    async def tbmatrix(self, context: Context, *args, subreddit: str = None, startDate: str = None, endDate: str = None):
        '''
        Generate a toolbox mod matrix for <subreddit> from <startDate> [optional] to <endDate> [optional]

        Parameters:
            subreddit: Subreddit you want the matrix for (note: not needed in sub channel)
            startDate: Start date to get actions from. If not specified first day of current month is used. If the date has a space `"` or `'` are needed.
            endDate: End date to get actions from. If not specified current day is used. If the date has a space `"` or `'` are needed.

        Note: you can use a wide variety of date formats (e.g., today, yesterday, may, 1/1/2019 1/1/19, mar 1, etc.)

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
        '''
        subreddit = await self.getSubFromChannel(context)
        if len(args) >= 3:
            subreddit, startDate, endDate = args[:3]
        elif len(args) == 2:
            if subreddit:
                startDate, endDate = args[:2]
            else:
                subreddit, startDate = args[:2]
        elif len(args) == 1:
            if subreddit:
                startDate = args[0]
            else:
                subreddit = args[0]
        async with context.typing():
            if subreddit:
                try:
                    sub = await self.reddit.subreddit(subreddit)
                    self.bot.start_task(context, self.genMatrix(context, sub, startDate, endDate, tb=True))
                except asyncprawcore.exceptions.NotFound:
                    await self.error_embed(context, f'r/pics was not found.')
                    return
            else:
                await self.error_embed(context, 'Please use this command in a sub channel or specify sub.')
                return

    @command(aliases=['ms'])
    async def modstats(self, context, *mods):
        '''
        Gets moderator stats for [mod(s)]

        Parameters:
            moderator(s): a single moderator or moderators separated by spaces

        Examples:
         ```.ms``` for yourself
         ```.ms Lil_SpazJoekp```
         ```.ms Lil_SpazJoekp N8theGr8```
        '''
        async with context.typing():
            msg1 = await context.send("Checking Reddit.. This shouldn't take long")
            if mods:
                for user in mods:
                    try:
                        redditor = await self.reddit.redditor(user)
                    except asyncprawcore.NotFound:
                        await self.error_embed(context, f'Could not find [u/{user}](https://reddit.com/u/{user}). Either the account has been deleted or it does not exist.')
                        return
                    else:
                        await self.calculateAndSend(msg1, redditor.name, context)
                        return
            else:
                author = context.message.author
                redditor = await self.getUser(context, author)
                if redditor:
                    await self.calculateAndSend(msg1, redditor, context)
                else:
                    await self.error_embed(context, "Please use `.modstats <user>` or verify your Reddit account with `.verify`.")
                    return

    @command()
    async def traffic(self, context, *subreddits: str):
        '''
        Gets traffic for [subreddit(s)]

        Parameters:
            subreddit(s): a single subreddit or subreddits separated by spaces

        Examples:
         ```.traffic``` in sub channel
         ```.traffic dankmemes```
         ```.traffic dankmemes memes```
        '''
        async with context.typing():
            if subreddits:
                for subreddit in subreddits:
                    await self.parseTraffic(subreddit, context)
            else:
                subreddit = await self.getSubFromChannel(context)
                if subreddit:
                    await self.parseTraffic(subreddit, context)
                else:
                    await self.error_embed(context, 'Please use this command in a sub channel or specify sub.')

    @command(aliases=['lma'])
    async def lastmodaction(self, context, *moderators):
        '''
        Gets last mod action for yourself or `moderator(s)`

        Parameters:
            moderators: a single moderator or moderators separated by spaces

        Examples:
         ```.lastmodaction``` for yourself
         ```.lastmodaction Lil_SpazJoekp```
         ```.lastmodaction Lil_SpazJoekp siouxsie_siouxv2```
        '''
        async with context.typing():
            # globalMods, subs, multis = await self.parseLmaKwargs(moderators)
            if not moderators:
                author = context.message.author
                userID = author.id
                encodedAuthor = hashlib.sha256(str(userID).encode('utf-8')).hexdigest()
                results = parse_sql(await self.sql.fetch('SELECT * FROM verified WHERE id=$1', encodedAuthor))
                if results:
                    moderators = [results[0].redditor]
                else:
                    await self.error_embed(context, "Please a moderator or verify your Reddit account with `.verify`.")
            for moderator in moderators:
                mod = await self.getmod(context, moderator)
                if mod:
                    results = parse_sql(await self.sql.fetch('SELECT * FROM mirror.modlog WHERE moderator=$1 ORDER BY created_utc DESC LIMIT 1', mod))
                    if results:
                        lastAction = results[0]
                        embed = self.genActionEmbed(lastAction)
                        await context.send(embed=embed)
                    else:
                        await self.error_embed(context, f'Could not find any action performed by [u/{mod}](https://reddit.com/user/{mod})')

    async def parseLmaKwargs(self, context, args):
        """

        :param args:
        :return:
        """
        kwargs = {}
        validKwargs = ['sub', 'subreddit', 'action', 'multi', 'multireddit']
        kwargMapping = {'sub': 'sub', 'subreddit': 'sub', 'action': 'action', 'multi': 'multi', 'multireddit': 'multi'}
        validKwargStr = ', '.join([arg if not validKwargs.index(arg) + 1 == len(validKwargs) else f'and {arg}' for arg in validKwargs])
        subGroups = {}
        multiGroups = {}
        regex = r"((sub|subreddit|multi|multireddit)=(.*?( |$)))"
        argStr = ' '.join(args)
        mods = []
        groups = []
        for arg in args:
            if any([i in arg for i in validKwargs]):
                if arg == 'multi' or arg == 'multireddit':
                    mods = []
                #mods = []
            else:
                mods.append(arg)

        for arg in args:
            splitArg = arg.split('=')
            splitArg.reverse()
            kwarg = splitArg.pop()
            splitArg.reverse()
            if kwarg in validKwargs:
                kwargs[kwargMapping[kwarg]] = ''.join(splitArg)
            else:
                # print(f"{kwarg} isn't a valid argument. Valid arguments are: {validKwargStr}\nSkipping...")
                await self.error_embed(context, f"{kwarg} isn't a valid argument. Valid arguments are: {validKwargStr}\nSkipping...")
        return kwargs

    async def parseTraffic(self, subreddit, context):
        daystr = 'Date,Uniques,Pageviews,Subscriptions'
        hourstr = 'Date,Uniques,Pageviews'
        monthstr = 'Date,Uniques,Pageviews'
        subResults = await self.checkSub(context, subreddit)
        if subResults:
            subreddit = subResults['subreddit']
            sub = await self.reddit.subreddit(subreddit)
            subreddit = sub.display_name
            if subResults['isMod']:
                traffic = await sub.traffic()
                for day in traffic['day']:
                    date = genDateString(day[0], True, '%m/%d/%Y')
                    daystr += f'\n{date},{day[1]},{day[2]},{day[3]}'
                with open(f'Daily Traffic Stats for pics.csv', 'w') as csv:
                    csv.write(daystr)

                for hour in traffic['hour']:
                    date = genDateString(hour[0], True, '%m/%d/%Y %I %p %Z')
                    hourstr += f'\n{date},{hour[1]},{hour[2]}'
                with open(f'Hourly Traffic Stats for pics.csv', 'w') as csv:
                    csv.write(hourstr)

                for month in traffic['month']:
                    date = genDateString(month[0], True, '%B %Y')
                    monthstr += f'\n{date},{month[1]},{month[2]}'
                with open(f'Monthly Traffic Stats for pics.csv', 'w') as csv:
                    csv.write(monthstr)
                files = [discord.File(f'Daily Traffic Stats for pics.csv'), discord.File(f'Hourly Traffic Stats for pics.csv'), discord.File(f'Monthly Traffic Stats for pics.csv')]
                await context.send(files=files)
                for file in files:
                    os.remove(file.fp)
            else:
                await self.error_embed(context, f"You don't moderate r/pics")

    async def parseKwargs(self, args, context):
        kwargs = {}
        validKwargs = ['url', 'user', 'redditor', 'mod', 'moderator', 'sub', 'subreddit', 'limit', 'timezone', 'action']
        kwargMapping = {'url': 'url', 'mod': 'mod', 'user': 'user', 'redditor': 'user', 'sub': 'sub', 'limit': 'limit', 'timezone': 'timezone', 'moderator': 'mod', 'subreddit': 'sub', 'action': 'action'}
        validKwargStr = ', '.join([arg if not validKwargs.index(arg) + 1 == len(validKwargs) else f'and {arg}' for arg in validKwargs])
        for arg in args:
            splitArg = arg.split('=')
            splitArg.reverse()
            kwarg = splitArg.pop()
            splitArg.reverse()
            if kwarg in validKwargs:
                if kwarg == 'action':
                    if splitArg[0] in actionMapping:
                        kwargs[kwargMapping[kwarg]] = ''.join(splitArg)
                    else:
                        await self.error_embed(context, f'{splitArg} is not a valid action. Do `.validactions` to see valid actions.\nSkipping...')
                else:
                    kwargs[kwargMapping[kwarg]] = ''.join(splitArg)
            else:
                await self.error_embed(context, f"{kwarg} isn't a valid argument. Valid arguments are: {validKwargStr}\nSkipping...")
        return kwargs

    async def checkUrl(self, context, url: str):
        self.log.info('Checking url')
        reddit = self.reddit
        userRegex = r'https?:\/\/(www\.|old\.|new\.)?reddit\.com\/(user|u)/.*?$'
        match = re.search(userRegex, url, re.MULTILINE)
        if match:
            user = urllib3.util.parse_url(match[0]).path
            if user[-1] == '/':
                user = user[:-1].split('/')[-1]
            else:
                user = user.split('/')[-1]
            try:
                redditor = await reddit.redditor(user)
                return redditor
            except asyncprawcore.NotFound:
                await self.error_embed(context, f'Could not find u/{user}')
        else:
            urlRegex = r'https?:\/\/(((www\.|old\.|new\.)?reddit\.com)|redd\.it)\/'
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

    async def generateCSV(self, context, sql, filename, timezonestr, args):
        """

        :param sql:
        :param filename:
        :param timezonestr:
        :param args:
        """
        self.log.info('Generating CSV')
        columnMapping = {'created_utc': 'Date', 'moderator': 'Moderator', 'subreddit': 'Subreddit', 'mod_action': 'Action', 'details': 'Details', 'description': 'Description', 'target_author': 'Actioned Item Author', 'target_body': 'Actioned Item Body', 'target_type': 'Actioned Item Type', 'target_id': 'Actioned Item ID', 'target_permalink': 'Actioned Item Permalink', 'target_title': 'Actioned Item Title'}
        results = await sql.fetch(*args)
        csvheader = [columnMapping[column] for column in list(results[0].keys()) if column in columnMapping]
        rows = [tuple(list(result.values())) for result in results]
        try:
            if timezonestr.lower() == 'utc':
                tz = pytz.timezone('utc')
            else:
                tz = pytz.timezone(f'US/{timezonestr.title()}')
            csvheader[0] += f' {tz}'
            rows = [[column.astimezone(tz).strftime('%m/%d/%Y %I:%M:%S %p') if index == 0 else [column, (column or '').replace('\n', '\\n')][bool(column)] for index, column in enumerate(row)] for row in rows]
            if len(rows) > 0:
                self.log.info('Sending File')
                sheetManager = SpreadsheetManager()
                results = parse_sql(await self.sql.fetch('SELECT email FROM sioux.spreadsheet_users'))
                emails = [i.email for i in results]
                return sheetManager.processAndUpload(filename, csvheader, rows, 'ModLogs', emails)
            else:
                await self.error_embed(context, 'No logs found')
        except pytz.exceptions.UnknownTimeZoneError:
            await self.error_embed(context, f"{timezonestr} isn't a valid timezone")

    async def getModlog(self, context, url: str = None, mod: str = None, user: str = None, sub=None, limit: int = None, timezone: str = 'Eastern', action=None):
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
                    self.log.info('Getting Logs')
                    sqlstr = "SELECT created_utc, moderator, subreddit, mod_action, details, description, target_author, target_body, target_type, target_id, target_permalink, target_title FROM mirror.modlog"
                    subreddit = None
                    global argIndex
                    argIndex = 1
                    parts = [sqlstr]
                    values = []
                    names = []
                    suffix = 'modlog.csv'

                    def nextArg(arg, value):
                        global argIndex
                        nextIndex = lambda i: parts.append(f'{i}=${argIndex}')
                        if len(parts) > 1:
                            parts.append('AND')
                        else:
                            parts.append('WHERE')
                        nextIndex(arg)
                        values.append(value)
                        argIndex += 1

                    try:
                        if limit:
                            limit = int(limit)
                    except:
                        await self.error_embed(context, 'Limit must be a whole number, skipping limit')
                    if user or url or mod or sub or action:
                        if user:
                            userRegex = r'https?:\/\/(www\.|old\.|new\.)?reddit\.com\/(user|u)/.*?$'
                            match = re.search(userRegex, user, re.MULTILINE)
                            if match:
                                thing = await self.checkUrl(context, user)
                            else:
                                thing = await self.checkUrl(context, f'https://reddit.com/u/{user}')
                            if thing:
                                nextArg('target_author', thing.name)
                                names.append(thing.name)
                        elif url:
                            thing = await self.checkUrl(context, url)
                            if thing:
                                if isinstance(thing, asyncpraw.models.Redditor):
                                    nextArg('target_author', thing.name)
                                    names.append(thing.name)
                                else:
                                    nextArg('target_id', thing.id)
                                    names.append(thing.id)
                        if mod:
                            modname = await self.getmod(context, mod)
                            nextArg('moderator', modname)
                            names.append(modname)
                        if sub:
                            nextArg('subreddit', sub)
                            names.append(sub)
                        if action:
                            nextArg('mod_action', action)
                            names.append(action)
                        parts.append('ORDER BY created_utc DESC')
                        if limit:
                            parts.append(f'LIMIT ${argIndex}')
                            values.append(limit)
                        sqlStatement = await sql.prepare((' '.join(parts)))
                        filename = '_'.join(names + [suffix])
                        link = await self.generateCSV(context, sqlStatement, filename, timezone, tuple(values))
                        await context.send(f'Hey {context.author.mention}, got your requested modlogs logs. You can view it here: {link}')
                    else:
                        await self.error_embed(context, 'Please include at least one of the following:```url, mod, user, limit, action, or sub```or run this command in a sub specific channel')
        except CancelledError:
            pass

    async def genMatrix(self, context, subreddit, startingDate=None, endingDate=None, tb=False):
        try:
            async with context.typing():
                async with context.acquire() as sql:
                    embed = Embed(title='Generating Matrix')
                    embed.add_field(name='Subreddit', value=subreddit.display_name)
                    if self.checkDateErrored:
                        self.checkDateErrored = False
                        return
                    startDate = await self.checkDate(context, startingDate, currentMonth=True)
                    endDate = await self.checkDate(context, endingDate, today=True)
                    if endDate < startDate:
                        await self.error_embed(context, 'Start date must be before end date')
                        return
                    data = (subreddit.display_name, startDate, endDate)
                    embed.add_field(name='Starting Date', value=startDate.strftime(f'%B {ordinal(startDate.day)}, %Y'))
                    embed.add_field(name='Ending Date', value=endDate.strftime(f'%B {ordinal(endDate.day)}, %Y'))
                    msg = await context.send(embed=embed)
                    if tb:
                        thingTypes = {'t1': 'Comment', 't2': 'Account', 't3': 'Link', 't4': 'Message', 't5': 'Subreddit', 't6': 'Award'}
                        startEpoch = startDate.timestamp()
                        endEpoch = endDate.timestamp()
                        # noinspection PyTypeChecker
                        Result = NamedTuple('Result', [('moderator', str), ('mod_action', str), ('target_type', str)])
                        results = []
                        reddit: asyncpraw.Reddit = subreddit._reddit
                        i = 0
                        after = None
                        responseCount = 500
                        endWhile = False
                        while responseCount == 500:
                            if after:
                                params = {'limit': 500, 'after': after}
                            else:
                                params = {'limit': 500}
                            modlog = await reddit.get(f'r/{subreddit.display_name}/about/log', params=params)
                            actions = [action for action in modlog]
                            responseCount = len(actions)
                            after = actions[-1].id
                            for action in actions:
                                fields = (
                                    {'name': 'Subreddit', 'value': subreddit.display_name},
                                    {'name': 'Starting Date', 'value': startDate.strftime(f'%B {ordinal(startDate.day)}, %Y')},
                                    {'name': 'Ending Date', 'value': endDate.strftime(f'%B {ordinal(endDate.day)}, %Y')},
                                    {'name': 'Counted Actions', 'value': f'{i:,}'},
                                    {'name': 'Current Action Date', 'value': time.strftime('%m/%d/%Y %I:%M:%S %p', time.localtime(action.created_utc))}
                                )
                                if i == 1 or i % 1000 == 0 and i != 0:
                                    msg = await self.statusUpdateEmbed(msg, 'Getting mod actions...', *fields)
                                if startEpoch <= action.created_utc <= endEpoch:
                                    i += 1
                                    thingType = None
                                    if action.target_fullname:
                                        thingType = thingTypes[action.target_fullname.split('_')[0]]
                                    logAction = Result(moderator=action._mod, mod_action=action.action, target_type=thingType)
                                    results.append(logAction)
                                    self.log.info(f'{i:,} :: {logAction}')
                                elif action.created_utc > endEpoch:
                                    continue
                                else:
                                    msg.embeds[0].color = discord.Color.green()
                                    msg = await self.statusDoneEmbed(msg, 'Done', *fields)
                                    endDate = datetime.fromtimestamp(action.created_utc, tz=dt.timezone.utc)
                                    endWhile = True
                                    break
                            if endWhile:
                                break
                    else:
                        query = await asyncpg.utils._mogrify(sql, "SELECT moderator, mod_action FROM mirror.modlog WHERE subreddit=$1 and created_utc > $2 and created_utc < $3;", data)
                        self.log.debug(query)
                        results = await sql.fetch("SELECT moderator, mod_action, target_type FROM mirror.modlog WHERE subreddit=$1 and created_utc > $2 and created_utc < $3;", *data, timeout=10000)
                        results = parse_sql(results)
                    action_types = set()
                    action_types = self._simply_mod_actions(action_types, results)
                    mods = {result.moderator: {action: 0 for action in action_types} for result in results}
                    subreddit = await self.reddit.subreddit('pics')
                    subMods = await subreddit.moderator()
                    for mod in subMods:
                        mods[mod.name] = {action: 0 for action in action_types}
                    for result in results:
                        mods[result.moderator][self._simplify_action(result)] += 1
                    df = pandas.DataFrame(mods)
                    df.loc['Total'] = df.sum()
                    df = df.transpose()
                    df = df.sort_values('Total', ascending=False)
                    df.loc['Total'] = df.sum()
                    df = df.transpose()
                    df = df.drop('Total')
                    df = df.sort_values('Total', ascending=False)
                    df.loc['Total'] = df.sum()
                    df = df.transpose()
                    total_column = df.pop('Total')
                    df.insert(0, 'Total', total_column)
                    # df = df.loc[:, (df != 0).any(axis=0)]
                    filename = f'{subreddit.display_name}-matrix-{startDate.strftime("%m/%d/%Y")}-to-{endDate.strftime("%m/%d/%Y")}'
                    matrix = io.BytesIO()
                    dataframe_image.export(df, matrix, max_cols=-1, table_conversion='matplotlib')
                    matrix = io.BytesIO(matrix.getvalue())

                    await context.send(f'Hey {context.author.mention}, here is your mod matrix:', files=[discord.File(matrix, filename=filename + '.png'), discord.File(io.BytesIO(df.to_csv().encode()), filename=filename + '.csv')])
        except CancelledError:
            await self.cancelledEmbed(context, msg)
        except Exception as error:
            self.log.error(error)

    def _simply_mod_actions(self, action_types, results):
        for result in results:
            mod_action = self._simplify_action(result)
            action_types.add(mod_action)
        return action_types

    @staticmethod
    def _simplify_action(result):
        mod_action = result.mod_action
        if 'sticky' in mod_action or 'lock' in mod_action:
            mod_action += {'submission': 'link', 'link': 'link', 'comment': 'comment'}[result.target_type.lower()]
        return mod_action

    async def checkDate(self, context, date, *, lastMonth=False, currentMonth=False, today=False):
        if date:
            if isinstance(date, int):
                if 1 <= date <= 12:
                    parsedDate = parseDate(date)
                else:
                    await self.error_embed(context, f'{date} is not a valid date, please use a number between 1 and 12 or a valid date')
                    self.checkDateErrored = True
                    return
            else:
                parsedDate = parseDate(date)
                if not parsedDate:
                    await self.error_embed(context, f'{date} is not a valid date, please use a number between 1 and 12 or a valid date')
                    self.checkDateErrored = True
                    return
        else:
            if today:
                parsedDate = parseDate(time.strftime('%m/%d/%Y %I:%M:%S %p', time.gmtime()))
            else:
                if lastMonth:
                    months = 1
                if currentMonth:
                    months = 0
                parsedDate = parseDate(time.strftime('%m', time.gmtime(datetime.timestamp(datetime.today() - dateutil.relativedelta.relativedelta(months=months)))))
        return parsedDate

    def genEmbed(self, user, subCount, subscribers, subAverage, remaining, zeroCount):
        embed = discord.Embed(title=f'Moderated Subreddit Stats for u/{user}', url=f'https://www.reddit.com/user/{user}')
        embed.add_field(name='Reddit Username', value=user)
        embed.add_field(name='Subreddit Count', value=f'{subCount:,}')
        embed.add_field(name='Subscriber Count', value=f'{subscribers:,}')
        embed.add_field(name='Avg. Subscriber Count', value=f'{subAverage:,}')
        embed.add_field(name='Subreddits with 1 Subs', value=f'{remaining:,}')
        embed.add_field(name='Subreddits with 0 Subs', value=f'{zeroCount:,}')
        return embed

    async def calculateAndSend(self, msg1, user, context):
        subredditsJson = await self.getModeratedSubreddits(context, user)
        subreddits = [(i['sr'], i['subscribers']) for i in subredditsJson['data']]
        subscribers = sum([subreddit[1] for subreddit in subreddits])
        subCount = len(subreddits)
        zeroCount = len([subreddit[1] for subreddit in subreddits if subreddit[1] == 0])
        remaining = len([subreddit[1] for subreddit in subreddits if subreddit[1] == 1])
        subAverage = int(round(subscribers / len(subreddits)))
        embed = self.genEmbed(user, subCount, subscribers, subAverage, remaining, zeroCount)
        valueString = '\n'.join([f'{subRank}. {subreddit[0]}: {subreddit[1]:,}' for subRank, subreddit in enumerate(subreddits[:20], 1)])
        embed.add_field(name='Top 20 Subreddits', value=valueString, inline=False)
        results = parse_sql(await self.sql.fetch('SELECT * FROM moderators WHERE redditor ilike $1', user))
        if results:
            redditor = results[0]
            user = redditor.redditor
            formattedTime = datetime.astimezone(redditor.updated).strftime("%B %d, %Y at %I:%M:%S %p %Z")
            previousSubscriberCount = redditor.subscribers
            previousSubCount = redditor.subreddits
            embed.set_footer(text=f'{subCount - previousSubCount:+,} Subreddits and {subscribers - previousSubscriberCount:+,} Subscribers since I last checked on {formattedTime}')
        else:
            embed.set_footer(text=f'{subCount:+,} Subreddits and {subscribers:+,} Subscribers')
        data = (user, subCount, subscribers)
        await self.sql.execute('INSERT INTO moderators(redditor, subreddits, subscribers) VALUES($1, $2, $3) ON CONFLICT (redditor) DO UPDATE SET subreddits=excluded.subreddits, subscribers=excluded.subscribers', *data)
        await msg1.delete()
        await context.send(embed=embed)

    def genActionEmbed(self, action):
        embed = Embed(title='Most Recent Mod Action', description=f'Last action performed by [u/{action.moderator}](https://reddit.com/user/{action.moderator})')
        embed.add_field(name="Action", value=actionMapping[action.mod_action])
        embed.add_field(name='Action Date/Time', value=action.created_utc.astimezone(pytz.timezone('US/Pacific')).strftime('%B %d, %Y at %I:%M:%S %p %Z'))
        if getattr(action, 'subreddit', None):
            subreddit = f'[{action.subreddit}](https://reddit.com/r/{action.subreddit})'
        else:
            subreddit = 'None'
        embed.add_field(name="Subreddit", value=subreddit)
        embed.add_field(name="Details", value=getattr(action, 'details', 'None'))
        embed.add_field(name="Description", value=getattr(action, 'description', 'None'))
        if getattr(action, 'target_title', None) and getattr(action, 'target_permalink', None):
            embed.add_field(name="Target", value=f'[{action.target_title}](https://reddit.com{action.target_permalink})')
        else:
            embed.add_field(name="Target", value='None')
        if getattr(action, 'target_author', None) and getattr(action, 'target_author', None) != '':
            if getattr(action, 'target_author', None):
                targetAuthor = f'[{action.target_author}](https://reddit.com/user/{action.target_author})'
            else:
                targetAuthor = 'None'
            embed.add_field(name="Target Author", value=targetAuthor)

        if action.target_body:
            bodySections = textwrap.wrap(action.target_body, 1021)
            if len(bodySections) == 1:
                embed.add_field(name="Target Body", value=f'{bodySections[0]}')
            else:
                embed.add_field(name="Target Body", value=f'{bodySections[0]}...')
        score = None
        self.log.debug(embed.to_dict())
        return embed


def setup(bot):
    bot.add_cog(RedditStats(bot))