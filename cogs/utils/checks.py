from discord.ext import commands

# The permission system of the bot is based on a "just works" basis
# You have permissions and the bot has permissions. If you meet the permissions
# required to execute the command (and the bot does as well) then it goes through
# and you can execute the command.
# Certain permissions signify if the person is a moderator (Manage Server) or an
# admin (Administrator). Having these signify certain bypasses.
# Of course, the owner will always be able to execute commands.


async def check_permissions(context, perms, *, check=all):
    is_owner = await context.bot.is_owner(context.author)
    if is_owner:
        return True

    resolved = context.channel.permissions_for(context.author)
    return check(
        getattr(resolved, name, None) == value for name, value in perms.items()
    )


def has_permissions(*, check=all, **perms):
    async def pred(context):
        return await check_permissions(context, perms, check=check)

    return commands.check(pred)


async def check_guild_permissions(context, perms, *, check=all):
    is_owner = await context.bot.is_owner(context.author)
    if is_owner:
        return True

    if context.guild is None:
        return False

    resolved = context.author.guild_permissions
    return check(
        getattr(resolved, name, None) == value for name, value in perms.items()
    )


def has_guild_permissions(*, check=all, **perms):
    async def pred(context):
        return await check_guild_permissions(context, perms, check=check)

    return commands.check(pred)


# These do not take channel overrides into account


def is_mod():
    async def pred(context):
        return await check_guild_permissions(context, {"manage_guild": True})

    return commands.check(pred)


def is_admin():
    async def pred(context):
        return await check_guild_permissions(context, {"administrator": True})

    return commands.check(pred)


def mod_or_permissions(**perms):
    perms["manage_guild"] = True

    async def predicate(context):
        return await check_guild_permissions(context, perms, check=any)

    return commands.check(predicate)


def admin_or_permissions(**perms):
    perms["administrator"] = True

    async def predicate(context):
        return await check_guild_permissions(context, perms, check=any)

    return commands.check(predicate)


def is_in_guilds(*guild_ids):
    def predicate(context):
        guild = context.guild
        if guild is None:
            return False
        return guild.id in guild_ids

    return commands.check(predicate)
