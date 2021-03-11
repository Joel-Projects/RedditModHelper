import json
import logging
import re

import discord
from discord import Embed
from discord.ext import commands
from discord.ext.commands import MemberConverter, RoleConverter, TextChannelConverter

from .utils import db
from .utils.command_cog import CommandCog
from .utils.commands import command

log = logging.getLogger("root")


class BotConfig(db.Table, table_name="settings"):
    key = db.Column(db.String, primary_key=True, unique=True, nullable=False)
    value = db.Column(db.JSON, nullable=False)


class Settings(CommandCog):
    """Settings"""

    async def cog_check(self, context):
        return await self.bot.is_owner(context.author)

    async def getType(self, context, value):
        converter = None
        if isinstance(value, str):
            if value.startswith("["):
                values = value.strip("[]").split(",")
                return [await self.getType(context, value) for value in values]
            elif value.startswith("{"):
                removeExtraSpaces = re.compile(r"\s*([\{\}:,\[\]])\s*")
                quoteWords = re.compile(r"([\{\[,:])*(\w)([,\]+:\}])")
                spacesRemoved = removeExtraSpaces.sub("\\1", value)
                if spacesRemoved:
                    result = quoteWords.sub('\\1"\\2"\\3', spacesRemoved)
                    if result:
                        finalResult = result.replace("'", '"').replace(' "', ' \\"').replace('" ', '\\" ')
                        loadedJson = json.loads(finalResult)
                        if loadedJson:
                            finalJson = {}
                            for key, value in loadedJson.items():
                                finalJson[key] = await self.getType(context, value)
                            return finalJson
            elif value.startswith("("):
                values = value.strip("()").split(",")
                return [await self.getType(context, value) for value in values]
            elif value.lower() == "true":
                return True
            elif value.lower() == "false":
                return False
            elif value.isdigit():
                return int(value)
            elif value.startswith("<@&"):
                converter = RoleConverter()
            elif value.startswith(("<@", "<@!")):
                converter = MemberConverter()
            elif value.startswith(("<#", "<#!")):
                converter = TextChannelConverter()
            if converter:
                item = await converter.convert(context, value)
                if item:
                    return item.id
        elif isinstance(value, list):
            return [await self.getType(context, i) for i in value]
        elif isinstance(value, tuple):
            return [await self.getType(context, i) for i in list(value)]
        elif isinstance(value, dict):
            return await self.getType(context, str(value))
        return value

    def sql_select(self, context, setting=None):
        if setting:
            return context.db.fetch("SELECT * FROM settings WHERE key=$1", setting)
        else:
            return context.db.fetch("SELECT * FROM settings")

    @command(hidden=True)
    async def get(self, context, *args):
        """
        Gets a bot setting

        Usage:
            .get <setting (Case-Sensitive)>

            Example:
                .get settingName
        """
        argsCount = len(args)
        if not argsCount == 0:
            embed = Embed(title="Get Bot Config")
            for setting in args:
                if setting == "all":
                    results = await self.sql_select(context)
                else:
                    results = await self.sql_select(context, setting)
                results = [(key.get("key"), json.loads(key.get("value"))) for key in [result for result in results]]
                if len(results) > 0:
                    result = results[0]
                    value = result[1]["value"]
                    embed.add_field(
                        name=f"{result[0]} ({value.__class__.__name__})",
                        value=f"```{value}```",
                    )
                else:
                    await self.error_embed(context, f"Could not find setting: `{setting}`")
                    return
            await self.success_embed(context, embed)
        else:
            await self.error_embed(context, "This command requires at least one setting to lookup")

    @command(hidden=True)
    async def set(self, context, *args):
        """
        Sets a bot setting

        Usage:
            .set <setting (Case-Sensitive)> <value (Case-Sensitive)>

            Example:
                .set settingName value

        Note:
            You can list multiple settings and values separated with spaces.

            Example:
                .set setting1 value1 setting2 value2 setting3 value3
        """
        keys = args[0::2]
        values = args[1::2]
        argsCount = len(args)
        toBeCommitted = {}
        if not argsCount == 0:
            if argsCount % 2 == 0:
                settings = [(keys[index], values[index]) for index in range(0, len(keys))]
                embed = Embed(title="Set Bot Config")
                for setting in settings:
                    key = setting[0]
                    configValue = await self.getType(context, setting[1])
                    if setting[1] == "all":
                        await self.error_embed(context, "Setting: `all` is not valid")
                        return
                    else:
                        toBeCommitted[key] = configValue
                        embed.add_field(
                            name=f"{key} ({configValue.__class__.__name__})",
                            value=f"```{configValue}```",
                        )
                await self.set_bot_config(**toBeCommitted)
                await self.success_embed(context, embed)
            else:
                await self.error_embed(context, "This command requires even number of arguments")
        else:
            await self.error_embed(
                context,
                "This command requires at least one setting and value pair to set",
            )

    @command(aliases=["a"], hidden=True)
    async def append(self, context, *args):
        """
        Appends an item to a bot list setting

        Usage:
            ```.append <setting (Case-Sensitive)> <comma separated values>```

            Example:
                ```.append settingName value```

        Note:
            You can list multiple settings and values separated with spaces.

            Example:
                ```.append setting1 value1A,value1B setting2 value2A,value2B setting3 value3A,value3B```
        """
        keys = args[0::2]
        values = args[1::2]
        argsCount = len(args)
        toBeCommitted = {}
        if not argsCount == 0:
            if argsCount % 2 == 0:
                settings = [(keys[index], values[index]) for index in range(0, len(keys))]
                embed = Embed(title="Appended Item(s) Successfully!")
                wasChanged = False
                for setting in settings:
                    key = setting[0]
                    value = await self.getType(context, setting[1])
                    existing = await self.get_bot_config(key)
                    if isinstance(existing, (list, tuple)):
                        if value in existing:
                            await self.error_embed(context, f"{value!r} is already in in {key!r}")
                        else:
                            if value == "all":
                                await self.error_embed(context, f"Setting: `all` is not valid")
                                continue
                            else:
                                existing.append(value)
                                wasChanged = True
                    else:
                        await self.error_embed(context, f"Setting: {key!r} is not a list or tuple!")
                        continue
                    if wasChanged:
                        toBeCommitted[key] = existing
                        embed.add_field(
                            name=f"{key} ({existing.__class__.__name__})",
                            value=f"```{existing}```",
                        )
                if len(embed.fields) > 0:
                    await self.set_bot_config(**toBeCommitted)
                    await self.success_embed(context, embed)
            else:
                await self.error_embed(context, "This command requires even number of arguments")
        else:
            await self.error_embed(
                context,
                "This command requires at least one setting and value pair to set",
            )

    @command(aliases=["r"], hidden=True)
    @commands.is_owner()
    async def remove(self, context, *args):
        """
        Removes an item from a bot list setting

        Usage:
            ```.remove <setting (Case-Sensitive)> <comma separated values>```

            Example:
                ```.remove settingName value```

        Note:
            You can list multiple settings and values separated with spaces.

            Example:
                ```.remove setting1 value1A,value1B setting2 value2A,value2B setting3 value3A,value3B```
        """
        keys = args[0::2]
        values = args[1::2]
        argsCount = len(args)
        toBeCommitted = {}
        if not argsCount == 0:
            if argsCount % 2 == 0:
                settings = [(keys[index], values[index]) for index in range(0, len(keys))]
                embed = Embed(title="Removed Item(s) Successfully!")
                wasChanged = False
                for setting in settings:
                    key = setting[0]
                    value = await self.getType(context, setting[1])
                    existing = await self.get_bot_config(key)
                    if isinstance(existing, (list, tuple)):
                        if len(existing) > 0:
                            if value in existing:
                                await self.error_embed(context, f"{value} is already in in {key}")
                            else:
                                if value == "all":
                                    await self.error_embed(context, f"Setting: `all` is not valid")
                                    continue
                                else:
                                    if isinstance(existing, tuple):
                                        convertToList = list(existing)
                                        convertToList.remove(value)
                                        existing = tuple(convertToList)
                                        wasChanged = True
                                    else:
                                        existing.remove(value)
                                        wasChanged = True
                        else:
                            await self.error_embed(context, f"Setting: {key!r} is not a list or tuple!")
                    else:
                        await self.error_embed(
                            context,
                            "This command requires the setting to already be set!",
                        )
                        continue
                    if wasChanged:
                        embed.add_field(
                            name=f"{key} ({existing.__class__.__name__})",
                            value=f"```{existing}```",
                        )
                if len(embed.fields) > 0:
                    await self.set_bot_config(**toBeCommitted)
                    await self.success_embed(context, embed)
            else:
                await self.error_embed(context, "This command requires even number of arguments")
        else:
            await self.error_embed(
                context,
                "This command requires at least one setting and value pair to set",
            )

    @command(aliases=["ds"], hidden=True)
    async def deletesetting(self, context, *args):
        """
        Deletes a bot setting

        Usage:

            .deletesetting <setting (Case-Sensitive)>
            .deletesetting all (Deletes ALL settings)

            Example:
                .deletesetting settingName
        Note:
            You can list multiple settings separated with spaces.

            Example:
                .deletesetting setting1 setting2 setting3
        """
        bot: discord.Client = self.bot
        self.sql = self.sql
        author = context.message.author

        def check(message):
            return author == message.author

        argsCount = len(args)
        canceled = False
        error = False
        settingsToDelete = []
        if not argsCount == 0:
            embed = Embed(title="Delete Bot Setting")
            for setting in args:
                if setting == "all":
                    results = await self.sql_select(context)
                    results = [(key.get("key"), json.loads(key.get("value"))) for key in [result for result in results]]
                    for result in results:
                        value = result[1]["value"]
                        embed.add_field(
                            name=f"{result[0]} ({value.__class__.__name__})",
                            value=f"```{value}```",
                        )
                    confirm, message = await context.prompt(
                        "Are you sure you want to delete the following settings?",
                        embed=embed,
                        delete_after=False,
                        return_message=True,
                    )
                    if confirm:
                        confirm, message = await context.prompt(
                            "Are you REALLY sure you want to delete the following settings?",
                            embed=embed,
                            delete_after=False,
                            return_message=True,
                        )
                        if confirm:
                            await context.db.execute("DELETE FROM settings")
                            embed = Embed(
                                title="Deleted all settings",
                                color=discord.Color.green(),
                            )
                        else:
                            canceled = True
                    else:
                        canceled = True
                else:
                    results = await self.sql_select(context, setting)
                    results = [(key.get("key"), json.loads(key.get("value"))) for key in [result for result in results]]
                    if len(results) > 0:
                        result = results[0]
                        settingsToDelete.append(setting)
                        value = result[1]["value"]
                        embed.add_field(
                            name=f"{result[0]} ({value.__class__.__name__})",
                            value=f"```{value}```",
                        )
                    else:
                        error = True
                        await self.error_embed(context, f"Could not find setting: `{setting}`")
            if not error:
                confirm, message = await context.prompt(
                    "Are you sure you want to delete the following settings?",
                    embed=embed,
                    delete_after=False,
                    return_message=True,
                )
                canceled = not confirm
                if confirm:
                    for setting in settingsToDelete:
                        await context.db.execute("DELETE FROM settings WHERE key=$1", setting)
                    embed = Embed(
                        title=f"Deleted {len(settingsToDelete)} setting(s)",
                        color=discord.Color.green(),
                    )
                else:
                    canceled = True
            if not canceled and not error:
                await self.success_embed(context, embed)
            elif canceled:
                embed = message.embeds[0]
                embed.title = "Canceled"
                embed.description = None
                embed.color = discord.Color.greyple()
                await message.edit(embed=embed)
        else:
            await self.error_embed(context, "This command requires at least one setting to lookup")


def setup(bot):
    bot.add_cog(Settings(bot))
