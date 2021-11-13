import asyncio
import os.path
from collections import defaultdict

import sys
import time
from datetime import datetime, timedelta
from itertools import zip_longest
from multiprocessing import freeze_support

import aiostream
import asyncpraw
import asyncprawcore
import pylibmc
from credmgr.exceptions import NotFound

from streams.tasks import ingest_action_chunk
from streams.utils import map_values, try_multiple

from . import cache, connection_pool, log, mapping, services, skip_keys
from .models import Subreddit, Webhook


class ModLogStreams:
    def __init__(self, reddit_params, subreddits, redditor):
        self.redditor = redditor
        self.subreddits = subreddits
        self.reddit = asyncpraw.Reddit(**reddit_params, timeout=30)
        self.killed = False

    async def _log_wrapper(self, subreddit, admin, stream):
        sub = await self.reddit.subreddit(subreddit)
        is_stream = True
        while is_stream:
            is_stream = stream
            try:
                if stream:
                    modlog = sub.mod.stream.log(mod=f"{'' if admin else '-'}a")
                else:
                    modlog = sub.mod.log(mod=f"{'' if admin else '-'}a", limit=None)
                async for action in modlog:
                    yield action, admin, stream
            except Exception as error:
                if (
                    error.response.status == 400
                    and error.response.reason == "Bad Request"
                    and str(error.response.url) == "https://www.reddit.com/api/v1/access_token"
                ):
                    log.error(f"Invalid auth for u/{self.redditor}, killing stream...")
                    self.killed = True
                    break
                else:
                    log.exception(error)

    async def run(self):
        while not self.killed:
            try:
                to_send = []
                last_action = time.time()
                modlogs = [
                    self._log_wrapper("+".join(self.subreddits), admin, stream)
                    for admin in [True, False]
                    for stream in [True, False]
                ]
                combine = aiostream.stream.merge(*modlogs)
                async with combine.stream() as modlog:
                    has_admin = False
                    async for action, admin, stream in modlog:
                        try:
                            data = map_values(action.__dict__, mapping, skip_keys)
                            new = try_multiple(
                                cache.add, (data["id"], 1), exception=pylibmc.Error, default_result=False
                            )
                            if new:
                                to_send.append([data, admin, stream])
                                has_admin = admin
                                log.info(
                                    f"Ingesting {data['subreddit']} | {data['moderator']} | {data['mod_action']} | {data['created_utc'].astimezone().strftime('%m-%d-%Y %I:%M:%S %p')}"
                                )
                            else:
                                log.debug(
                                    f"Already ingested {data['subreddit']} | {data['moderator']} | {data['mod_action']} | {data['created_utc'].astimezone().strftime('%m-%d-%Y %I:%M:%S %p')}"
                                )
                            if (
                                len(to_send) >= 500
                                or has_admin
                                or (
                                    (time.time() - last_action) > 10
                                )  # send if last new action was more than 10 seconds ago
                            ) and to_send:
                                if len(to_send) > 20:
                                    to_ingest = [to_send[x : x + 10] for x in range(0, len(to_send), 10)]
                                    log.info(f"Sending {len(to_ingest):,} chunks with {len(to_send):,} actions")
                                    for to_send in to_ingest:
                                        ingest_action_chunk.apply_async(
                                            args=(to_send,), priority=1, queue="action_chunks"
                                        )
                                else:
                                    log.info(f"Sending 1 chunk with {len(to_send):,} actions")
                                    ingest_action_chunk.apply_async(args=(to_send,), priority=1, queue="action_chunks")
                                to_send = []
                                has_admin = False
                            if new:
                                last_action = time.time()
                        except asyncprawcore.ServerError as error:
                            log.info(error)
                            log.info((self.subreddits, await self.reddit.user.me()))
                        except Exception as error:
                            log.exception(error)
            except Exception as error:
                log.exception(error)


def get_last_cache_reset():
    if not os.path.isfile("last_cache_reset"):
        return 86400
    else:
        with open("last_cache_reset", "r") as f:
            try:
                last_reset = float(f.read())
            except Exception:
                last_reset = time.time() - 86400
            return time.time() - last_reset


async def main():
    subreddits = Subreddit.query.all()
    accounts = defaultdict(set)
    for subreddit in subreddits:
        accounts[subreddit.modlog_account].add(subreddit.name)
    streams = []
    for redditor, subreddits in accounts.items():
        subreddits = list(subreddits)
        for chunk, subreddit_chunk in enumerate([subreddits[x : x + 3] for x in range(0, len(subreddits), 3)], 1):
            streams.append(start_streaming(subreddit_chunk, redditor, chunk))
    # if sys.platform != "darwin":
    #     subreddits = services.reddit("Lil_SpazJoekp").user.me().moderated()
    #     chunks = list(
    #         zip_longest(
    #             *[
    #                 reversed(chunk) if i % 2 == 0 else chunk
    #                 for i, chunk in enumerate([subreddits[x : x + 25] for x in range(0, len(subreddits), 25)])
    #             ]
    #         )
    #     )
    #     for chunk, subreddit_chunk in enumerate(chunks, 1):
    #         streams.append(
    #             start_streaming(
    #                 [sub.display_name for sub in subreddit_chunk if sub], "Lil_SpazJoekp", chunk, other_auth=True
    #             )
    #         )
    await asyncio.gather(*streams)


async def start_streaming(subreddits, redditor, chunk, other_auth=False):
    try:
        log.info(f"Building chunk {chunk} for r/{'+'.join(subreddits)} using u/{redditor}...")
        # if other_auth:
        #     reddit = services.reddit(redditor, bot_name=f"SiouxBot_Log_Thread_{chunk}")
        # else:
        reddit = services.reddit(redditor)
        reddit_params = reddit.config._settings
        subreddit_streams = ModLogStreams(reddit_params, subreddits, redditor)
        log.info(f"Starting streams for r/{'+'.join(subreddits)}")
        await subreddit_streams.run()
    except NotFound as error:
        log.exception(error)


def set_cache():
    log.info("Setting cache...")
    conn = connection_pool.getconn()
    sql = conn.cursor()
    days = 90
    log.info(f"Fetching last {days} days of ids...")
    beginning_time = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    subreddits = Subreddit.query.all()
    sql.execute(
        "SELECT id FROM mirror.modlog WHERE created_utc>=%s AND NOT subreddit=ANY(%s)",
        (beginning_time, [subreddit.name for subreddit in subreddits]),
    )
    results = sql.fetchall()
    connection_pool.putconn(conn)
    chunk_size = 50000
    chunks = [{result.id: 1 for result in results[x : x + chunk_size]} for x in range(0, len(results), chunk_size)]
    total_chunks = len(chunks)
    log.info(f"Caching {len(results):,} ids ({total_chunks:,} chunks)")
    for i, result_chunk in enumerate(chunks, 1):
        cache.set_multi(result_chunk)
        log.info(f"Chunk {i}/{total_chunks} set")
    log.info("Cache set")
    set_last_cache_reset()
    return [result.id for result in results]


def set_last_cache_reset():
    with open("last_cache_reset", "w") as f:
        f.write(f"{time.time()}")


def set_webhooks():
    to_set = {}
    results = Webhook.query.session.execute(
        """SELECT
      subreddit,
      array_to_string(array_agg(distinct admin_webhook),',') AS admin_webhooks,
      array_to_string(array_agg(distinct alert_webhook),',') AS alert_webhooks
    FROM webhooks GROUP BY subreddit;"""
    )
    subreddits = results.fetchall()
    for subreddit, admin, alert in subreddits:
        to_set[f"{subreddit}_admin_webhooks"] = admin.split(",")
        to_set[f"{subreddit}_alert_webhooks"] = alert.split(",")
    try:
        cache.set_multi(to_set)
    except Exception:
        pass


if __name__ == "__main__":
    freeze_support()
    loop = asyncio.get_event_loop()
    try:
        if get_last_cache_reset() >= 86400:
            cache.flush_all()
            set_cache()
        set_webhooks()
        asyncio.run(main())
    except Exception as error:
        log.exception(error)
    except KeyboardInterrupt:
        pass
