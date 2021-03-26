import asyncio
import io
import time

import discord
from discord import Embed
from discord.ext import commands


class _ContextDBAcquire:
    __slots__ = ("context", "timeout")

    def __init__(self, context, timeout):
        self.context = context
        self.timeout = timeout

    def __await__(self):
        return self.context._acquire(self.timeout).__await__()

    async def __aenter__(self):
        await self.context._acquire(self.timeout)
        return self.context.db

    async def __aexit__(self, *args):
        await self.context.release()


class Context(commands.Context):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pool = self.bot.pool
        self._db = None

    async def entry_to_code(self, entries):
        width = max(len(a) for a, b in entries)
        output = ["```"]
        for name, entry in entries:
            output.append(f"{name:<{width}}: {entry}")
        output.append("```")
        await self.send("\n".join(output))

    async def indented_entry_to_code(self, entries):
        width = max(len(a) for a, b in entries)
        output = ["```"]
        for name, entry in entries:
            output.append(f"\u200b{name:>{width}}: {entry}")
        output.append("```")
        await self.send("\n".join(output))

    def __repr__(self):
        # we need this for our cache key strategy
        return "<Context>"

    @property
    def session(self):
        return self.bot.session

    @discord.utils.cached_property
    def replied_reference(self):
        ref = self.message.reference
        if ref and isinstance(ref.resolved, discord.Message):
            return ref.resolved.to_reference()
        return None

    async def ask(self, message):
        await self.send(message)

        def message_check(msg):
            return msg.author.id == self.author.id and self.channel == msg.channel

        try:
            answer: discord.Message = await self.bot.wait_for("message", check=message_check, timeout=30.0)
            if answer:
                if answer.content.lower() == "!c":
                    raise asyncio.CancelledError
                return answer.content
        except asyncio.TimeoutError:
            await self.send("Took too long.", delete_after=5)

    async def disambiguate(self, matches, entry):
        if len(matches) == 0:
            raise ValueError("No results found.")

        if len(matches) == 1:
            return matches[0]

        await self.send("There are too many matches... Which one did you mean? **Only say the number**.")
        await self.send("\n".join(f"{index}: {entry(item)}" for index, item in enumerate(matches, 1)))

        def check(m):
            return m.content.isdigit() and m.author.id == self.author.id and m.channel.id == self.channel.id

        await self.release()

        # only give them 3 tries.
        try:
            for i in range(3):
                try:
                    message = await self.bot.wait_for("message", check=check, timeout=30.0)
                except asyncio.TimeoutError:
                    raise ValueError("Took too long. Goodbye.")

                index = int(message.content)
                try:
                    return matches[index - 1]
                except:
                    await self.send(f"Please give me a valid number. {2 - i} tries remaining...")

            raise ValueError("Too many tries. Goodbye.")
        finally:
            await self.acquire()

    async def prompt(
        self,
        message,
        *,
        timeout=60.0,
        embed=None,
        delete_after=True,
        reacquire=False,
        author_id=None,
        sendEmbed=True,
        color: discord.Color = discord.Color.orange(),
        return_message=False,
    ):
        """An interactive reaction confirmation dialog.

        Parameters
        -----------
        message: str
            The message to show along with the prompt.
        timeout: float
            How long to wait before returning.
        delete_after: bool
            Whether to delete the confirmation message after we're done.
        reacquire: bool
            Whether to release the database connection and then acquire it
            again when we're done.
        author_id: Optional[int]
            The member who should respond to the prompt. Defaults to the author of the
            Context's message.
        sendEmbed:

        Returns
        --------
        Optional[bool]
            ``True`` if explicit confirm,
            ``False`` if explicit deny,
            ``None`` if deny due to timeout
        """

        if not self.channel.permissions_for(self.guild.me if self.guild is not None else self.bot.user).add_reactions:
            raise RuntimeError("Bot does not have Add Reactions permission.")

        fmt = f"{message}\n\nReact with \N{WHITE HEAVY CHECK MARK} to confirm or \N{CROSS MARK} to deny."

        author_id = author_id or self.author.id
        if sendEmbed:
            if not embed:
                embed = Embed(title="Confirmation Needed")
            embed.color = color
            embed.description = fmt
            embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
            msg = await self.send(embed=embed)
        else:
            msg = await self.send(fmt)
        confirm = None

        def check(payload):
            nonlocal confirm

            if payload.message_id != msg.id or payload.user_id != author_id or payload.user_id != 393801572858986496:
                return False

            codepoint = str(payload.emoji)

            if codepoint == "\N{WHITE HEAVY CHECK MARK}":
                confirm = True
                return True
            elif codepoint == "\N{CROSS MARK}":
                confirm = False
                return True

            return False

        for emoji in ("\N{WHITE HEAVY CHECK MARK}", "\N{CROSS MARK}"):
            await msg.add_reaction(emoji)

        if reacquire:
            await self.release()

        try:
            await self.bot.wait_for("raw_reaction_add", check=check, timeout=timeout)
        except asyncio.TimeoutError:
            confirm = None

        try:
            if reacquire:
                await self.acquire()

            if delete_after:
                await msg.delete()
        finally:
            if return_message:
                return confirm, msg
            else:
                return confirm

    def tick(self, opt, label=None):
        emoji = "<:greenTick:646449055005671444>" if opt else "<:redTick:646449054946951194>"
        if label is not None:
            return f"{emoji}: {label}"
        return emoji

    @property
    def db(self):
        return self._db if self._db else self.pool

    async def _acquire(self, timeout):
        if self._db is None:
            self._db = await self.pool.acquire(timeout=timeout)
        return self._db

    def acquire(self, *, timeout=None):
        """Acquires a database connection from the pool. e.g. ::

            async with context.acquire():
                await context.db.execute(...)

        or: ::

            await context.acquire()
            try:
                await context.db.execute(...)
            finally:
                await context.release()
        """
        return _ContextDBAcquire(self, timeout)

    async def release(self):
        """Releases the database connection from the pool.

        Useful if needed for "long" interactive commands where
        we want to release the connection and re-acquire later.

        Otherwise, this is called automatically by the bot.
        """
        # from source digging asyncpg source, releasing an already
        # released connection does nothing

        if self._db is not None:
            await self.bot.pool.release(self._db)
            self._db = None

    async def show_help(self, command=None):
        """Shows the help command for the specified command if given.

        If no command is given, then it'll show help for the current
        command.
        """
        cmd = self.bot.get_command("help")
        command = command or self.command.qualified_name
        await self.invoke(cmd, command=command)

    async def safe_send(self, content, *, escape_mentions=True, **kwargs):
        """Same as send except with some safe guards.

        1) If the message is too long then it sends a file with the results instead.
        2) If ``escape_mentions`` is ``True`` then it escapes mentions.
        """
        if escape_mentions:
            content = discord.utils.escape_mentions(content)

        if len(content) > 2000:
            fp = io.BytesIO(content.encode())
            kwargs.pop("file", None)
            return await self.send(file=discord.File(fp, filename="message_too_long.txt"), **kwargs)
        else:
            return await self.send(content)
