import itertools
import textwrap
import time
from copy import copy

from discord import Embed
from praw.models import ListingGenerator


def convert_or_none(func):
    def wrapper(arg):
        if arg:
            return func(arg)

    return wrapper


def map_values(action_dict, mapping, skip_keys=None):
    if skip_keys is None:
        skip_keys = []
    data = {}
    for key, value in action_dict.items():
        if key not in skip_keys and value is not None:
            item_mapping = mapping.get(key, None)
            if isinstance(item_mapping, str):
                data[item_mapping] = value
            elif item_mapping is None:
                data[key] = value
            elif isinstance(item_mapping, dict):
                data.update(
                    map_values({sub_key: value for sub_key in item_mapping.keys()}, item_mapping), **{key: value}
                )
            else:
                data[key] = item_mapping(value)
    return data


class MemcacheHelper:
    def __init__(self, client):
        self.client = client

    def __getattr__(self, item):
        try:
            return self.__getattribute__(item)
        except AttributeError:
            return getattr(self.client, item)

    def append(self, key, value):
        stored = self.client.get(key)
        result = False
        if stored:
            if value not in stored:
                stored.append(value)
                self.client.set(key, stored)
                result = True
        else:
            self.client.set(key, [value])
            result = True
        return result


def gen_action_embed(action):

    embed = Embed()

    embed.add_field(name="Action", value=action["mod_action"])
    if "Anti-Evil Operations" == action["moderator"] or "Reddit Legal" == action["moderator"]:
        embed.title = "AEO Action" if "Anti-Evil Operations" == action["moderator"] else "Reddit Legal Action"
        embed.add_field(name="Moderator", value=action["moderator"])
    else:
        embed.title = "Admin Action"
        embed.add_field(
            name="Moderator", value=f"[u/{action['moderator']}](https://reddit.com/user/{action['moderator']})"
        )
    embed.add_field(name="Details", value=action.get("details", None) or "None")
    embed.add_field(name="Description", value=action.get("description", None) or "None")
    title = action.get("target_title", None)
    permalink = action.get("target_permalink", None)
    if title and permalink:
        embed.add_field(name="Target", value=f"[{title}](https://reddit.com{permalink})")
    elif title:
        embed.add_field(name="Target Title", value=title)
    elif permalink:
        embed.add_field(name="Permalink", value=f"[permalink](https://reddit.com{permalink})")
    if action.get("target_author", None):
        target_author = f"[u/{action['target_author']}](https://reddit.com/user/{action['target_author']})"
    else:
        target_author = "None"
    embed.add_field(name="Target Author", value=target_author)
    get_more = False
    if action.get("target_body", None):
        bodySections = textwrap.wrap(action["target_body"], 1021)
        if len(bodySections) == 1:
            embed.add_field(name="Target Body", value=f"{bodySections[0]}")
        else:
            embed.add_field(name="Target Body", value=f"{bodySections[0]}...")
            get_more = True
    embed.set_footer(text=action["created_utc"].astimezone().strftime("%B %d, %Y at %I:%M:%S %p %Z"))
    return embed, get_more


def generate_sub_chunks(subreddits, chunk_size=25):
    modded_subs = copy(subreddits)
    modded_subs_final = copy(subreddits)
    modded_subs_final.sort()
    i = True
    while i:
        first_section = [modded_subs[x : x + chunk_size] for x in range(0, len(modded_subs), chunk_size)][0]
        if "dankmemes" in first_section and len(modded_subs) > chunk_size:
            i = "dankmemes" in first_section
        else:
            i = False
        modded_subs = [
            sub
            for tup in [
                i
                for i in itertools.zip_longest(
                    modded_subs[: len(modded_subs) // 2], reversed(modded_subs[len(modded_subs) // 2 :])
                )
            ]
            for sub in tup
            if sub
        ]
    return modded_subs, modded_subs_final


class ChunkGenerator(ListingGenerator):
    def __next__(self):
        """Permit ListingGenerator to operate as a generator."""
        if self.limit is not None and self.yielded >= self.limit:
            raise StopIteration()

        if self._listing is None or self._list_index >= len(self._listing):
            self._next_batch()

        self._list_index += len(self._listing)
        self.yielded += len(self._listing)
        return self._listing


def try_multiple(func, args=None, kwargs=None, wait_time=3, max_attempts=3, default_result=None, exception=Exception):
    from . import log

    if not args:
        args = ()
    if not kwargs:
        kwargs = {}
    try:
        result = func(*args, **kwargs)
    except exception as error:
        attempts = 0
        while attempts < max_attempts:
            try:
                result = func(*args, **kwargs)
                break
            except exception:
                pass
            log.warning(f"Waiting {wait_time} seconds before trying again")
            time.sleep(wait_time)
            attempts += 1
        else:
            log.exception(error)
            result = default_result
    return result
