import sys

from discord.ext.commands import Command, Group, GroupMixin


class DiscordBotCommand(Command):
    def __init__(self, callback, **kwargs):
        super().__init__(callback, **kwargs)

        if sys.platform == "darwin":
            self.aliases += [
                f"{alias}2"
                for alias in self.aliases[:] + [self.name]
                if not f"{alias}2" in self.aliases
            ]


def command(name=None, cls=None, **attrs):
    if cls is None:
        cls = DiscordBotCommand

    def decorator(func):
        if isinstance(func, Command):
            raise TypeError("Callback is already a command.")
        return cls(func, name=name, **attrs)

    return decorator


class DiscordBotGroup(Group, GroupMixin, DiscordBotCommand):
    def __init__(self, *args, **attrs):
        super().__init__(*args, **attrs)


def group(name=None, **attrs):
    attrs.setdefault("cls", DiscordBotGroup)
    return command(name=name, **attrs)
