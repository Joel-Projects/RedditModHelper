from discord_slash import SlashCommand as _SlashCommand
from discord_slash import SlashContext as _SlashContext
from discord_slash import error


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
