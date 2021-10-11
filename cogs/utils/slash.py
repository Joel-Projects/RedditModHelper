from discord_slash.client import SlashCommand as _SlashCommand


class SlashCommand(_SlashCommand):
    def __init__(self, *args, **kwargs):
        self.debug_mode = False
        super().__init__(*args, **kwargs)

    async def invoke_command(self, *args, **kwargs):
        if not self.debug_mode:
            await super().invoke_command(*args, **kwargs)

    async def invoke_component_callback(self, *args, **kwargs):
        if not self.debug_mode:
            await super().invoke_component_callback(*args, **kwargs)
