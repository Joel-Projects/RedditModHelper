import textwrap

import praw
from discord import Embed

from cogs.utils.utils import gen_date_string


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


def gen_action_embed(action: praw.models.ModAction):

    embed = Embed()

    embed.add_field(name="Action", value=action.action)
    if "Anti-Evil Operations" == action._mod or "Reddit Legal" == action._mod:
        embed.title = "AEO Action" if "Anti-Evil Operations" == action._mod else "Reddit Legal Action"
        embed.add_field(name="Moderator", value=action._mod)
    else:
        embed.title = "Admin Action"
        embed.add_field(name="Moderator", value=f"[u/{action._mod}](https://reddit.com/user/{action._mod})")
    embed.add_field(name="Details", value=getattr(action, "details", "None"))
    embed.add_field(name="Description", value=getattr(action, "description", "None"))
    title = getattr(action, "target_title", None)
    permalink = getattr(action, "target_permalink", None)
    if title and permalink:
        embed.add_field(name="Target", value=f"[{title}](")
    elif title:
        embed.add_field(name="Target Title", value=title)
    elif permalink:
        embed.add_field(name="Permalink", value=f"[permalink](https://reddit.com{permalink})")
    # if score:
    #     embed.add_field(name="Target Score", value=f"{score:,}")
    if getattr(action, "target_author", None):
        target_author = f"[u/{action.target_author}](https://reddit.com/user/{action.target_author})"
    else:
        target_author = "None"
    embed.add_field(name="Target Author", value=target_author)
    get_more = False
    if action.target_body:
        bodySections = textwrap.wrap(action.target_body, 1021)
        if len(bodySections) == 1:
            embed.add_field(name="Target Body", value=f"{bodySections[0]}")
        else:
            embed.add_field(name="Target Body", value=f"{bodySections[0]}...")
            get_more = True
    embed.set_footer(text=gen_date_string(epoch=action.created_utc))
    return embed, get_more
