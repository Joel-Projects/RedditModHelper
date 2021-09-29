import asyncio
import copy
import importlib
import inspect
import io
import os
import re
import subprocess
import sys
import textwrap
import time
import traceback
from contextlib import redirect_stdout
from typing import Optional

import discord
from discord.ext import commands

from .utils.command_cog import CommandCog
from .utils.commands import command


class PerformanceMocker:
    """A mock object that can also be used in await expressions."""

    def __init__(self):
        self.loop = asyncio.get_event_loop()

    def permissions_for(self, obj):
        # Lie and say we don't have permissions to embed
        # This makes it so pagination sessions just abruptly end on __init__
        # Most checks based on permission have a bypass for the owner anyway
        # So this lie will not affect the actual command invocation.
        perms = discord.Permissions.all()
        perms.administrator = False
        perms.embed_links = False
        perms.add_reactions = False
        return perms

    def __getattr__(self, attr):
        return self

    def __call__(self, *args, **kwargs):
        return self

    def __repr__(self):
        return "<PerformanceMocker>"

    def __await__(self):
        future = self.loop.create_future()
        future.set_result(self)
        return future.__await__()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return self

    def __len__(self):
        return 0

    def __bool__(self):
        return False


class GlobalChannel(commands.Converter):
    async def convert(self, context, argument):
        try:
            return await commands.TextChannelConverter().convert(context, argument)
        except commands.BadArgument:
            # Not found... so fall back to ID + global lookup
            try:
                channel_id = int(argument, base=10)
            except ValueError:
                raise commands.BadArgument(f"Could not find a channel by ID {argument!r}.")
            else:
                channel = context.bot.get_channel(channel_id)
                if channel is None:
                    raise commands.BadArgument(f"Could not find a channel by ID {argument!r}.")
                return channel


class Admin(CommandCog, command_attrs=dict(hidden=True)):
    """Admin-only commands that make the bot dynamic."""

    def __init__(self, bot):
        super().__init__(bot)
        self._last_result = None
        self.sessions = set()

    async def run_process(self, command):
        try:
            process = await asyncio.create_subprocess_shell(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            result = await process.communicate()
        except NotImplementedError:
            process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            result = await self.bot.loop.run_in_executor(None, process.communicate)

        return [output.decode() for output in result]

    def cleanup_code(self, content):
        """Automatically removes code blocks from the code."""
        # remove ```py\n```
        if content.startswith("```") and content.endswith("```"):
            return "\n".join(content.split("\n")[1:-1])

        # remove `foo`
        return content.strip("` \n")

    async def cog_command_error(self, context, error):
        if isinstance(error, commands.CheckFailure):
            if error.args[0][32:-8] == "sudo":
                await context.send("<@393801572858986496> sudo command was used")
                spaz = context.guild.get_member(
                    393801572858986496,
                )
                embed = discord.Embed(title="Sudo command was used", color=discord.Color.red())
                embed.add_field(name="User", value=context.message.author.mention)
                embed.add_field(
                    name="Command",
                    value=f"[{context.message.content}]({context.message.jump_url})",
                )
                embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
                spaz.send(embed=embed)

    async def cog_check(self, context):
        return await self.bot.is_owner(context.author)

    def get_syntax_error(self, e):
        if e.text is None:
            return f"```py\n{e.__class__.__name__}: {e}\n```"
        return f'```py\n{e.text}{"^":>{e.offset}}\n{e.__class__.__name__}: {e}```'

    @command()
    async def load(self, context, *, module):
        """Loads a module."""
        try:
            self.bot.load_extension(module)
        except commands.ExtensionError as e:
            await context.send(f"{e.__class__.__name__}: {e}")
        else:
            await context.send("\N{OK HAND SIGN}")

    @command()
    async def unload(self, context, *, module):
        """Unloads a module."""
        try:
            self.bot.unload_extension(module)
        except commands.ExtensionError as e:
            await context.send(f"{e.__class__.__name__}: {e}")
        else:
            await context.send("\N{OK HAND SIGN}")

    @commands.group(name="reload", invoke_without_command=True)
    async def _reload(self, context, *, module):
        """Reloads a module."""
        try:
            self.bot.reload_extension(module)
        except commands.ExtensionError as e:
            await context.send(f"{e.__class__.__name__}: {e}")
        else:
            await context.send("\N{OK HAND SIGN}")

    _GIT_PULL_REGEX = re.compile(r"\s*(?P<filename>.+?)\s*\|\s*[0-9]+\s*[+-]+")

    def find_modules_from_git(self, output):
        files = self._GIT_PULL_REGEX.findall(output)
        ret = []
        for file in files:
            root, ext = os.path.splitext(file)
            if ext != ".py":
                continue

            if root.startswith("cogs/"):
                # A submodule is a directory inside the main cog directory for
                # my purposes
                ret.append((root.count("/") - 1, root.replace("/", ".")))

        # For reload order, the submodules should be reloaded first
        ret.sort(reverse=True)
        return ret

    def reload_or_load_extension(self, module):
        try:
            self.bot.reload_extension(module)
        except commands.ExtensionNotLoaded:
            self.bot.load_extension(module)

    @_reload.command(
        name="all",
    )
    async def _reload_all(self, context):
        """Reloads all modules, while pulling from git."""

        async with context.typing():
            stdout, stderr = await self.run_process("git pull")

        # progress and stuff is redirected to stderr in git pull
        # however, things like "fast forward" and files
        # along with the text "already up-to-date" are in stdout

        if stdout.startswith("Already up-to-date."):
            return await context.send(stdout)

        modules = self.find_modules_from_git(stdout)
        mods_text = "\n".join(f"{index}. `{module}`" for index, (_, module) in enumerate(modules, start=1))
        prompt_text = f"This will update the following modules, are you sure?\n{mods_text}"
        confirm = await context.prompt(prompt_text)
        if not confirm:
            return await context.send("Aborting.")

        statuses = []
        for is_submodule, module in modules:
            if is_submodule:
                try:
                    actual_module = sys.modules[module]
                except KeyError:
                    statuses.append((context.tick(None), module))
                else:
                    try:
                        importlib.reload(actual_module)
                    except Exception as e:
                        statuses.append((context.tick(False), module))
                    else:
                        statuses.append((context.tick(True), module))
            else:
                try:
                    self.reload_or_load_extension(module)
                except commands.ExtensionError:
                    statuses.append((context.tick(False), module))
                else:
                    statuses.append((context.tick(True), module))

        await context.send("\n".join(f"{status}: `{module}`" for status, module in statuses))

    @command(pass_context=True, name="eval")
    async def _eval(self, context, *, body: str):
        """Evaluates a code"""

        env = {
            "bot": self.bot,
            "context": context,
            "channel": context.channel,
            "author": context.author,
            "guild": context.guild,
            "message": context.message,
            "_": self._last_result,
        }

        env.update(globals())

        body = self.cleanup_code(body)
        stdout = io.StringIO()

        to_compile = f'async def func():\n{textwrap.indent(body, "  ")}'

        try:
            exec(to_compile, env)
        except Exception as e:
            return await context.send(f"```py\n{e.__class__.__name__}: {e}\n```")

        func = env["func"]
        try:
            with redirect_stdout(stdout):
                ret = await func()
        except Exception as e:
            value = stdout.getvalue()
            await context.send(f"```py\n{value}{traceback.format_exc()}\n```")
        else:
            value = stdout.getvalue()
            try:
                await context.message.add_reaction("\u2705")
            except:
                pass

            if ret is None:
                if value:
                    await context.send(f"```py\n{value}\n```")
            else:
                self._last_result = ret
                await context.send(f"```py\n{value}{ret}\n```")

    @command(
        pass_context=True,
    )
    async def repl(self, context):
        """Launches an interactive REPL session."""
        variables = {
            "context": context,
            "bot": self.bot,
            "message": context.message,
            "guild": context.guild,
            "channel": context.channel,
            "author": context.author,
            "_": None,
        }

        if context.channel.id in self.sessions:
            await context.send("Already running a REPL session in this channel. Exit it with `quit`.")
            return

        self.sessions.add(context.channel.id)
        await context.send("Enter code to execute or evaluate. `exit()` or `quit` to exit.")

        def check(m):
            return m.author.id == context.author.id and m.channel.id == context.channel.id and m.content.startswith("`")

        while True:
            try:
                response = await self.bot.wait_for("message", check=check, timeout=10.0 * 60.0)
            except asyncio.TimeoutError:
                await context.send("Exiting REPL session.")
                self.sessions.remove(context.channel.id)
                break

            cleaned = self.cleanup_code(response.content)

            if cleaned in ("quit", "exit", "exit()"):
                await context.send("Exiting.")
                self.sessions.remove(context.channel.id)
                return

            executor = exec
            if cleaned.count("\n") == 0:
                # single statement, potentially 'eval'
                try:
                    code = compile(cleaned, "<repl session>", "eval")
                except SyntaxError:
                    pass
                else:
                    executor = eval

            if executor is exec:
                try:
                    code = compile(cleaned, "<repl session>", "exec")
                except SyntaxError as e:
                    await context.send(self.get_syntax_error(e))
                    continue

            variables["message"] = response

            fmt = None
            stdout = io.StringIO()

            try:
                with redirect_stdout(stdout):
                    result = executor(code, variables)
                    if inspect.isawaitable(result):
                        result = await result
            except Exception as e:
                value = stdout.getvalue()
                fmt = f"```py\n{value}{traceback.format_exc()}\n```"
            else:
                value = stdout.getvalue()
                if result is not None:
                    fmt = f"```py\n{value}{result}\n```"
                    variables["_"] = result
                elif value:
                    fmt = f"```py\n{value}\n```"

            try:
                if fmt is not None:
                    if len(fmt) > 2000:
                        await context.send("Content too big to be printed.")
                    else:
                        await context.send(fmt)
            except discord.Forbidden:
                pass
            except discord.HTTPException as e:
                await context.send(f"Unexpected error: `{e}`")

    @command()
    async def sql(self, context, *, query: str):
        """Run some SQL."""
        # the imports are here because I imagine some people would want to use
        # this cog as a base for their other cog, and since this one is kinda
        # odd and unnecessary for most people, I will make it easy to remove
        # for those people.
        import time

        from .utils.formats import TabularData, plural

        query = self.cleanup_code(query)

        is_multistatement = query.count(";") > 1
        if is_multistatement:
            # fetch does not support multiple statements
            strategy = context.db.execute
        else:
            strategy = context.db.fetch

        try:
            start = time.perf_counter()
            results = await strategy(query)
            dt = (time.perf_counter() - start) * 1000.0
        except Exception:
            return await context.send(f"```py\n{traceback.format_exc()}\n```")

        rows = len(results)
        if is_multistatement or rows == 0:
            return await context.send(f"`{dt:.2f}ms: {results}`")

        headers = list(results[0].keys())
        table = TabularData()
        table.set_columns(headers)
        table.add_rows(list(r.values()) for r in results)
        render = table.render()

        fmt = f"```\n{render}\n```\n*Returned {plural(rows):row} in {dt:.2f}ms*"
        if len(fmt) > 2000:
            fp = io.BytesIO(fmt.encode("utf-8"))
            await context.send("Too many results...", file=discord.File(fp, "results.txt"))
        else:
            await context.send(fmt)

    @command()
    async def sql_table(self, context, *, table_name: str):
        """Runs a query describing the table schema."""
        from .utils.formats import TabularData

        query = """SELECT column_name, data_type, column_default, is_nullable FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = $1"""

        results = await context.db.fetch(query, table_name)

        headers = list(results[0].keys())
        table = TabularData()
        table.set_columns(headers)
        table.add_rows(list(r.values()) for r in results)
        render = table.render()

        message = f"```\n{render}\n```"
        if len(message) > 2000:
            fp = io.BytesIO(message.encode("utf-8"))
            await context.send("Too many results...", file=discord.File(fp, "results.txt"))
        else:
            await context.send(message)

    @command(aliases=["su"])
    async def sudo(
        self,
        context,
        channel: Optional[GlobalChannel],
        who: discord.User,
        *,
        command: str,
    ):
        """Run a command as another user optionally in another channel."""
        msg = copy.copy(context.message)
        channel = channel or context.channel
        msg.channel = channel
        msg.author = (
            channel.guild.get_member(
                who.id,
            )
            or who
        )
        msg.content = context.prefix + command
        new_context = await self.bot.get_context(msg, cls=type(context))
        if new_context.command:
            new_context._db = context._db
            await self.bot.invoke(new_context)
        else:
            await self.error_embed(context, f"Command: `{command}` doesn't exist!")

    @command()
    async def do(self, context, times: int, *, command):
        """Repeats a command a specified number of times."""
        msg = copy.copy(context.message)
        msg.content = context.prefix + command

        new_context = await self.bot.get_context(msg, cls=type(context))
        new_context._db = context._db

        for i in range(times):
            await new_context.reinvoke()

    @command()
    async def sh(self, context, *, command):
        """Runs a shell command."""
        from discord.ext.menus import MenuError

        from cogs.utils.paginator import RoboPages, TextPageSource

        async with context.typing():
            stdout, stderr = await self.run_process(command)

        if stderr:
            text = f"stdout:\n{stdout}\nstderr:\n{stderr}"
        else:
            text = stdout

        pages = RoboPages(TextPageSource(text))
        try:
            await pages.start(context)
        except MenuError as e:
            await context.send(str(e))

    @command()
    async def perf(self, context, *, command):
        """Checks the timing of a command, attempting to suppress HTTP and DB calls."""

        msg = copy.copy(context.message)
        msg.content = context.prefix + command

        new_context = await self.bot.get_context(msg, cls=type(context))
        new_context._db = PerformanceMocker()

        # Intercepts the Messageable interface a bit
        new_context._state = PerformanceMocker()
        new_context.channel = PerformanceMocker()

        if new_context.command is None:
            return await context.send("No command found")

        start = time.perf_counter()
        try:
            await new_context.command.invoke(new_context)
        except commands.CommandError:
            end = time.perf_counter()
            success = False
            try:
                await context.send(f"```py\n{traceback.format_exc()}\n```")
            except discord.HTTPException:
                pass
        else:
            end = time.perf_counter()
            success = True

        await context.send(f"Status: {context.tick(success)} Time: {(end - start) * 1000:.2f}ms")


def setup(bot):
    bot.add_cog(Admin(bot))
