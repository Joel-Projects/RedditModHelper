import inspect
import typing
from functools import partial

from discord.ext.commands import BadArgument, BadUnionArgument, CommandError, ConversionError
from discord.ext.commands import converter as converters
from discord_slash import SlashCommand as _SlashCommand
from discord_slash import SlashContext as _SlashContext
from discord_slash import error
from discord_slash.model import CogCommandObject as _CogCommandObject
from discord_slash.model import CogSubcommandObject as _CogSubcommandObject
from discord_slash.model import CommandObject, SubcommandObject
from discord_slash.utils import manage_commands

from cogs.utils.context import Context


class SlashContext(_SlashContext):
    async def defer(self, hidden: bool = False, embed=None, embeds=None):
        """
        'Deferes' the response, showing a loading state to the user

        :param embeds:
        :param embed:
        :param hidden: Whether the deffered response should be ephemeral . Default ``False``.
        """
        if embed and embeds:
            raise error.IncorrectFormat("You can't use both `embed` and `embeds`!")
        if embed:
            embeds = [embed]
        if embeds:
            if not isinstance(embeds, list):
                raise error.IncorrectFormat("Provide a list of embeds.")
            elif len(embeds) > 10:
                raise error.IncorrectFormat("Do not provide more than 10 embeds.")
        if self._deffered or self._sent:
            raise error.AlreadyResponded("You have already responded to this command!")
        base = {"type": 5}
        if hidden:
            base["data"] = {"flags": 64}
            self._deffered_hidden = True
        if embeds:
            base.setdefault("data", {})
            base["data"]["embeds"] = [x.to_dict() for x in embeds]
        await self._http.post_initial_response(base, self._interaction_id, self.__token)
        self._deffered = True


class SlashCommand(_SlashCommand):
    async def on_socket_response(self, msg):
        """
        This event listener is automatically registered at initialization of this class.

        .. warning::
            DO NOT MANUALLY REGISTER, OVERRIDE, OR WHATEVER ACTION TO THIS COROUTINE UNLESS YOU KNOW WHAT YOU ARE DOING.

        :param msg: Gateway message.
        """
        if msg["t"] != "INTERACTION_CREATE":
            return

        to_use = msg["d"]

        if to_use["data"]["name"] in self.commands:

            ctx = SlashContext(self.req, to_use, self._discord, self.logger)
            cmd_name = to_use["data"]["name"]

            if cmd_name not in self.commands and cmd_name in self.subcommands:
                return await self.handle_subcommand(ctx, to_use)

            selected_cmd = self.commands[to_use["data"]["name"]]

            if selected_cmd.allowed_guild_ids and ctx.guild_id not in selected_cmd.allowed_guild_ids:
                return

            if selected_cmd.has_subcommands and not selected_cmd.func:
                return await self.handle_subcommand(ctx, to_use)

            if "options" in to_use["data"]:
                for x in to_use["data"]["options"]:
                    if "value" not in x:
                        return await self.handle_subcommand(ctx, to_use)

            # This is to temporarily fix Issue #97, that on Android device
            # does not give option type from API.
            temporary_auto_convert = {}
            for x in selected_cmd.options:
                temporary_auto_convert[x["name"].lower()] = x["type"]

            args = (
                await self.process_options(
                    ctx.guild, to_use["data"]["options"], selected_cmd.connector, temporary_auto_convert
                )
                if "options" in to_use["data"]
                else {}
            )

            self._discord.dispatch("slash_command", ctx)

            await self.invoke_command(selected_cmd, ctx, args)


class CustomCommandObject(CommandObject):
    """
    Slash command object of this extension.

    .. warning::
        Do not manually init this model.

    :ivar name: Name of the command.
    :ivar func: The coroutine of the command.
    :ivar description: Description of the command.
    :ivar allowed_guild_ids: List of the allowed guild id.
    :ivar options: List of the option of the command. Used for `auto_register`.
    :ivar connector: Kwargs connector of the command.
    :ivar __commands_checks__: Check of the command.
    """

    async def _actual_conversion(self, ctx, converter, argument, param):
        if converter is bool:
            return argument

        try:
            module = converter.__module__
        except AttributeError:
            pass
        else:
            if module is not None and (module.startswith("discord.") and not module.endswith("converter")):
                return argument

        try:
            if inspect.isclass(converter):
                if issubclass(converter, converters.Converter):
                    instance = converter()
                    ret = await instance.convert(ctx, argument)
                    return ret
                else:
                    method = getattr(converter, "convert", None)
                    if method is not None and inspect.ismethod(method):
                        ret = await method(ctx, argument)
                        return ret
            elif isinstance(converter, converters.Converter):
                ret = await converter.convert(ctx, argument)
                return ret
        except CommandError:
            raise
        except Exception as exc:
            raise ConversionError(converter, exc) from exc

        try:
            return converter(argument)
        except CommandError:
            raise
        except Exception as exc:
            try:
                name = converter.__name__
            except AttributeError:
                name = converter.__class__.__name__

            raise BadArgument('Converting to "{}" failed for parameter "{}".'.format(name, param.name)) from exc

    async def convert(self, ctx, converter, argument, param):
        try:
            origin = converter.__origin__
        except AttributeError:
            pass
        else:
            if origin is typing.Union:
                errors = []
                _NoneType = type(None)
                for conv in converter.__args__:
                    # if we got to this part in the code, then the previous conversions have failed
                    # so we should just undo the view, return the default, and allow parsing to continue
                    # with the other parameters
                    if conv is _NoneType and param.kind != param.VAR_POSITIONAL:
                        ctx.view.undo()
                        return None if param.default is param.empty else param.default

                    try:
                        value = await self._actual_conversion(ctx, conv, argument, param)
                    except CommandError as exc:
                        errors.append(exc)
                    else:
                        return value

                # if we're  here, then we failed all the converters
                raise BadUnionArgument(param, converter.__args__, errors)

        return await self._actual_conversion(ctx, converter, argument, param)

    def _get_converter(self, param):
        converter = param.annotation
        if converter is param.empty:
            if param.default is not param.empty:
                converter = str if param.default is None else type(param.default)
            else:
                converter = str
        return converter


class CogCommandObject(_CogCommandObject, CustomCommandObject):
    """
    Slash command object but for Cog.

    .. warning::
        Do not manually init this model.
    """

    async def invoke(self, *args, **kwargs):
        """
        Invokes the command.

        :param args: Args for the command.
        :raises: .error.CheckFailure
        """
        can_run = await self.can_run(args[0])
        if not can_run:
            raise error.CheckFailure
        setattr(args[0], "cog", args[0].bot.slash.commands[args[0].command].cog)
        setattr(args[0], "prompt", partial(Context.prompt, args[0]))
        signature = inspect.signature(self.func)
        params = signature.parameters.copy()

        # PEP-563 allows postponing evaluation of annotations with a __future__
        # import. When postponed, Parameter.annotation will be a string and must
        # be replaced with the real value for the converters to work later on
        for key, value in params.items():
            if isinstance(value.annotation, str):
                params[key] = value.replace(annotation=eval(value.annotation, self.func.__globals__))
        for name, param in params.items():
            if name not in ["self", "context", "ctx"]:
                converter = self._get_converter(param)
                if name in kwargs:
                    kwargs[name] = await self.convert(args[0], converter, kwargs[name], param)
        return await self.func(self.cog, *args, **kwargs)


class CogSubcommandObject(_CogSubcommandObject, SubcommandObject):
    """
    Subcommand object but for Cog.

    .. warning::
        Do not manually init this model.
    """

    def __init__(self, *args):
        super().__init__(*args)
        self.cog = None  # Manually set this later.

    async def invoke(self, *args, **kwargs):
        """
        Invokes the command.

        :param args: Args for the command.
        :raises: .error.CheckFailure
        """
        can_run = await self.can_run(args[0])
        if not can_run:
            raise error.CheckFailure
        signature = inspect.signature(self.func)
        params = signature.parameters.copy()

        # PEP-563 allows postponing evaluation of annotations with a __future__
        # import. When postponed, Parameter.annotation will be a string and must
        # be replaced with the real value for the converters to work later on
        for key, value in params.items():
            if isinstance(value.annotation, str):
                params[key] = value = value.replace(annotation=eval(value.annotation, self.func.__globals__))
        for name, param in params:
            converter = self._get_converter(param)
            kwargs[name] = await self.convert(args[0], converter, kwargs[name], param)
        return await self.func(self.cog, *args, **kwargs)


def cog_slash(
    *,
    name: str = None,
    description: str = None,
    guild_ids=None,
    options: typing.List[dict] = None,
    connector: dict = None,
):
    """
    Decorator for Cog to add slash command.\n
    Almost same as :func:`.client.SlashCommand.slash`.

    Example:

    .. code-block:: python

        class ExampleCog(commands.Cog):
            def __init__(self, bot):
                self.bot = bot

            @cog_ext.cog_slash(name="ping")
            async def ping(self, ctx: SlashContext):
                await ctx.send(content="Pong!")

    :param name: Name of the slash command. Default name of the coroutine.
    :type name: str
    :param description: Description of the slash command. Default ``None``.
    :type description: str
    :param guild_ids: List of Guild ID of where the command will be used. Default ``None``, which will be global command.
    :type guild_ids: List[int]
    :param options: Options of the slash command. This will affect ``auto_convert`` and command data at Discord API. Default ``None``.
    :type options: List[dict]
    :param connector: Kwargs connector for the command. Default ``None``.
    :type connector: dict
    """
    if guild_ids is None:
        guild_ids = [785198941535731715, 521812393429303307]

    def wrapper(cmd):
        desc = description or inspect.getdoc(cmd)
        if options is None:
            opts = manage_commands.generate_options(cmd, desc, connector)
        else:
            opts = options

        _cmd = {
            "func": cmd,
            "description": desc,
            "guild_ids": guild_ids,
            "api_options": opts,
            "connector": connector,
            "has_subcommands": False,
        }
        return CogCommandObject(name or cmd.__name__, _cmd)

    return wrapper


def cog_subcommand(
    *,
    base,
    subcommand_group=None,
    name=None,
    description: str = None,
    base_description: str = None,
    base_desc: str = None,
    subcommand_group_description: str = None,
    sub_group_desc: str = None,
    guild_ids: typing.List[int] = None,
    options: typing.List[dict] = None,
    connector: dict = None,
):
    """
    Decorator for Cog to add subcommand.\n
    Almost same as :func:`.client.SlashCommand.subcommand`.

    Example:

    .. code-block:: python

        class ExampleCog(commands.Cog):
            def __init__(self, bot):
                self.bot = bot

            @cog_ext.cog_subcommand(base="group", name="say")
            async def group_say(self, ctx: SlashContext, text: str):
                await ctx.send(content=text)

    :param base: Name of the base command.
    :type base: str
    :param subcommand_group: Name of the subcommand group, if any. Default ``None`` which represents there is no sub group.
    :type subcommand_group: str
    :param name: Name of the subcommand. Default name of the coroutine.
    :type name: str
    :param description: Description of the subcommand. Default ``None``.
    :type description: str
    :param base_description: Description of the base command. Default ``None``.
    :type base_description: str
    :param base_desc: Alias of ``base_description``.
    :param subcommand_group_description: Description of the subcommand_group. Default ``None``.
    :type subcommand_group_description: str
    :param sub_group_desc: Alias of ``subcommand_group_description``.
    :param guild_ids: List of guild ID of where the command will be used. Default ``None``, which will be global command.
    :type guild_ids: List[int]
    :param options: Options of the subcommand. This will affect ``auto_convert`` and command data at Discord API. Default ``None``.
    :type options: List[dict]
    :param connector: Kwargs connector for the command. Default ``None``.
    :type connector: dict
    """
    base_description = base_description or base_desc
    subcommand_group_description = subcommand_group_description or sub_group_desc

    def wrapper(cmd):
        desc = description or inspect.getdoc(cmd)
        if options is None:
            opts = manage_commands.generate_options(cmd, desc, connector)
        else:
            opts = options

        _sub = {
            "func": cmd,
            "name": name or cmd.__name__,
            "description": desc,
            "base_desc": base_description,
            "sub_group_desc": subcommand_group_description,
            "guild_ids": guild_ids,
            "api_options": opts,
            "connector": connector,
        }
        return CogSubcommandObject(_sub, base, name or cmd.__name__, subcommand_group)

    return wrapper
