import asyncio
import enum
import os
import time
from functools import wraps
from inspect import getfullargspec
from typing import NamedTuple

import discord
import pytz
from tzlocal import get_localzone

os.environ["TZ"] = "America/Chicago"
time.tzset()
local_tz = get_localzone()


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)


def gen_date_string(epoch=time.time(), gmtime=False, format="%B %d, %Y at %I:%M:%S %p %Z"):
    if not gmtime:
        return time.strftime(format, time.localtime(epoch))
    else:
        return time.strftime(format, time.gmtime(epoch))


def ordinal(num):
    SUFFIXES = {1: "st", 2: "nd", 3: "rd"}
    if 10 <= num % 100 <= 20:
        suffix = "th"
    else:
        suffix = SUFFIXES.get(num % 10, "th")
    return str(num) + suffix


def parse_sql(results, fetch_one=False):
    if results:
        Result = NamedTuple(
            "Result",
            [
                i
                for i in zip(
                    [i for i in results[0].keys()],
                    [type(i) for i in results[0].values()],
                )
            ],
        )
        results = [Result(*result) for result in results]
    return results[0] if fetch_one and results else results


def resolve_sub(argument_name):
    def decorator(f):
        argspec = getfullargspec(f)
        argument_index = argspec.args.index(argument_name)

        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                value = args[argument_index]
                asyncio.get_event_loop().run_in_executor(get_sub(context))
                return f(*args, **kwargs)
            except Exception as error:
                from bot import log

                log.exception(error)

        return wrapper

    return decorator


def readable_list(items, inline_code=False):
    items = [f"`{str(s)}`" if inline_code else str(s) for s in items]
    if len(items) < 3:
        return " and ".join(items)
    return f"{', '.join(items[:-1])}, and {items[-1]}"


class EmbedType(enum.Enum):

    success = {"color": discord.Color.green(), "title": "Success!"}
    error = {"color": discord.Color.red(), "title": "Error!"}
    warning = {"color": discord.Color.orange(), "title": "Warning!"}


def generate_result_embed(message, result_type=EmbedType.success, title=None, contact_me=False):
    embed_kwargs = result_type.value
    if title:
        embed_kwargs["title"] = title
    embed_kwargs["description"] = message
    if contact_me:
        embed_kwargs["description"] += "\n\nIf you need more help, contact <@393801572858986496>."
    embed = discord.Embed(**embed_kwargs)
    embed.set_footer(text=time.strftime("%B %d, %Y at %I:%M:%S %p %Z", time.localtime()))
    return embed
