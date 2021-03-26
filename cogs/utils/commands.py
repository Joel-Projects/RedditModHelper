import sys

from discord.ext.commands import Command, Group, GroupMixin


class DiscordBotCommand(Command):
    def __init__(self, callback, **kwargs):
        super().__init__(callback, **kwargs)

        if sys.platform == "darwin":
            self.aliases += [f"{alias}2" for alias in self.aliases[:] + [self.name] if not f"{alias}2" in self.aliases]


def command(name=None, cls=None, **attrs):
    if cls is None:
        cls = DiscordBotCommand

    def decorator(func):
        if isinstance(func, Command):
            raise TypeError("Callback is already a command.")
        # description = attrs.get('description', None)
        # guild_ids = attrs.get('guild_ids', None)
        # options = attrs.get('options', None)
        # connector = attrs.get('connector', None)
        # desc = description or inspect.getdoc(func)
        # if options is None:
        #     opts = manage_commands.generate_options(func, desc, connector)
        # else:
        #     opts = options
        #
        # _cmd = {
        #     "func": func, "description": desc, "guild_ids": guild_ids, "api_options": opts, "connector": connector, "has_subcommands": False
        # }
        # CogCommandObject(name or func.__name__, _cmd)
        return cls(func, name=name, **attrs)

    return decorator


class DiscordBotGroup(Group, GroupMixin, DiscordBotCommand):
    def __init__(self, *args, **attrs):
        super().__init__(*args, **attrs)


def group(name=None, **attrs):
    attrs.setdefault("cls", DiscordBotGroup)
    return command(name=name, **attrs)
