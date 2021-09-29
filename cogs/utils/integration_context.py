import asyncio
import time

import discord
from discord import Embed
import discord_slash
from discord_slash import ButtonStyle
from discord_slash.context import (
    InteractionContext as _InteractionContext,
    SlashContext as _SlashContext,
    ComponentContext as _ComponentContext,
    MenuContext as _MenuContext,
)
from discord_slash.utils.manage_components import (
    create_actionrow,
    create_button,
    wait_for_component,
)

from cogs.utils.utils import EmbedType, generate_result_embed


async def _cleanup(context, message, buttons, confirm, delete_after, expired, hidden):
    if hidden:
        await context.send(
            embed=generate_result_embed("Confirmed: " + ("✅" if confirm else "❌"), result_type=EmbedType.success),
            hidden=hidden,
        )
    if delete_after:
        await message.delete()
    else:
        for row in buttons:
            for button in row["components"]:
                button["disabled"] = True
        message.embeds[0].color, message.embeds[0].title = {
            True: (discord.Color.green(), "Confirmed: ✅"),
            False: (discord.Color.red(), "Not Confirmed: ❌"),
            None: (discord.Color.greyple(), "Not Confirmed: Took too long" if expired else "Not Confirmed: Canceled"),
        }[confirm]
        await context.edit_origin(embeds=message.embeds, components=buttons)


class InteractionContext(_InteractionContext):
    async def prompt(
        self,
        confirmation_message,
        *,
        author_id=None,
        color: discord.Color = discord.Color.orange(),
        delete_after=True,
        embed=None,
        hidden=True,
        return_message=False,
        timeout=120.0,
    ):
        """An interactive reaction confirmation dialog.

        Parameters
        -----------
        confirmation_message: str
            The message to show along with the prompt.
        author_id: Optional[int]
            The member who should respond to the prompt. Defaults to the author of the
            Context's message.
        color: discord.Color
            The embed color.
        delete_after: bool
            Whether to delete the confirmation message after we're done.
        embed: discord.Embed
            An existing embed to use.
        hidden: bool
            Whether to make the confirmation ephemeral.
        return_message: bool
            Whether to return the sent confirmation message. This is ignored if `hidden`
            is set.
        timeout: float
            How long to wait before returning.

        Returns
        --------
        Optional[bool]
            ``True`` if explicit confirm,
            ``False`` if explicit deny,
            ``None`` if deny due to timeout
        """

        author_id = author_id or self.author.id
        if not embed:
            embed = Embed(title="Confirmation Needed")
        embed.color = color
        embed.description = confirmation_message
        embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
        buttons = [
            create_actionrow(
                create_button(style=ButtonStyle.green, emoji=discord.PartialEmoji(name="✔"), custom_id="yes"),
                create_button(style=ButtonStyle.danger, emoji=discord.PartialEmoji(name="✖"), custom_id="no"),
            )
        ]

        _message = await self.send(embed=embed, hidden=hidden, components=buttons)
        if isinstance(_message, discord_slash.model.SlashMessage):
            message = _message
        else:
            message = int(_message["id"])
        confirm = None
        expired = False
        try:

            def check(payload):
                nonlocal confirm

                if payload.author_id in [author_id, 393801572858986496]:
                    confirm = payload.custom_id == "yes"
                    return True
                return False

            button_context = await wait_for_component(
                self.bot, check=check, components=buttons, messages=message, timeout=timeout
            )
            if hidden:
                await button_context.defer(hidden=hidden)
        except asyncio.CancelledError:
            await _cleanup(self, message, buttons, confirm, delete_after, expired, hidden)
            return confirm
        except asyncio.TimeoutError:
            expired = True
            await self.send(embed=generate_result_embed("Took too long", result_type=EmbedType.error), hidden=True)
            button_context = self
        except Exception as error:
            self.bot.log.exception(error)
            await self.send(
                embed=generate_result_embed(
                    "An error occurred, please try again.", result_type=EmbedType.error, contact_me=True
                ),
                hidden=True,
            )
            button_context = self

        try:
            await _cleanup(button_context, message, buttons, confirm, delete_after, expired, hidden)
        finally:
            if return_message and not hidden:
                return confirm, message
            else:
                return confirm


class SlashContext(InteractionContext, _SlashContext):
    pass


class ComponentContext(InteractionContext, _ComponentContext):
    pass


class MenuContext(InteractionContext, _MenuContext):
    pass


discord_slash.context.InteractionContext = InteractionContext
discord_slash.context.SlashContext = SlashContext
discord_slash.context.ComponentContext = ComponentContext
discord_slash.context.MenuContext = MenuContext
