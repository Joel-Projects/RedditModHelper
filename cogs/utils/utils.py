import time
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


def parseSql(results):
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


