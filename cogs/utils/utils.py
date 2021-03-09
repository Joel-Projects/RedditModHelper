import asyncio
import time
from functools import wraps
from inspect import getfullargspec
from typing import NamedTuple


def genDateString(
    epoch=time.time(), gmtime=False, format="%B %d, %Y at %I:%M:%S %p %Z"
):
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


def parse_sql(results):
    if len(results) > 0:
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
        return results
    else:
        return None


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


# def resolve_sub(f):
#     @wraps(f)
#     def decorator(*args, **kwargs):
#         args_pec = getfullargspec(f)
#         context = args[args_pec.args.index('context')]
#         argument_index = args_pec.args.index('subreddit')
#         try:
#             value = args[argument_index]
#             asyncio.get_event_loop().run_in_executor(get_sub(context))
#             return f(*args, **kwargs)
#         except Exception as error:
#             from bot import log
#             log.exception(error)

# return decorator
