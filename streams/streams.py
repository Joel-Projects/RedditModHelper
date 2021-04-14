import sys
import time
from itertools import zip_longest
from multiprocessing import Process

import praw
import pylibmc
from credmgr.exceptions import NotFound

from streams.tasks import ingest_action
from streams.utils import map_values

from . import cache, connection_pool, log, mapping, services, skip_keys
from .models import Subreddit, Webhook


class ModLogStreams:
    STREAMS = ["admin_backlog", "admin_stream", "backlog", "stream"]

    def __init__(self, reddit_params, subreddit):
        self.reddit_params = reddit_params
        self.subreddit = subreddit

    # def _chunk(self, admin, modlog):
    #     for chunk in modlog:
    #         to_send = []
    #         mapped = list(
    #             map(partial(map_values, mapping=mapping, skip_keys=skip_keys), map(lambda item: item.__dict__, chunk))
    #         )
    #         to_ingest = self.check_cache_multi(mapped)
    #         for to_ingest_chunk in [to_ingest[x : x + 10] for x in range(0, len(to_ingest), 10)]:
    #             to_send.append([to_ingest_chunk, admin])
    #         ingest_action_chunk.chunks(to_send, 10).apply_async(priority=(1 if admin else 0), queue="action_chunks")

    def _stream(self, admin, modlog, stream):
        to_send = []
        while True:
            for action in modlog:
                try:
                    if action:
                        data = map_values(action.__dict__, mapping, skip_keys)
                        cached_id = self.check_cache(data)
                        if cached_id != action.id:
                            to_send.append([data, admin, stream])
                    if (len(to_send) % 500 == 0 or action is None) and to_send:
                        ingest_action.chunks(to_send, 10,).apply_async(
                            queue="actions",
                        )
                except Exception as error:
                    log.exception(error)
            if not stream:
                break

    @staticmethod
    def check_cache(data):
        try:
            cached_id = cache.get(data["id"])
        except pylibmc.Error:
            log.warning("Waiting 3 seconds before trying again")
            time.sleep(3)
            try:
                cached_id = cache.get(data["id"])
            except pylibmc.Error as error:
                log.exception(error)
                cached_id = None
                pass
        return cached_id

    @staticmethod
    def check_cache_multi(items):
        try:
            cached_items = cache.get_multi([item["id"] for item in items])
        except pylibmc.Error:
            log.warning("Waiting 3 seconds before trying again")
            time.sleep(3)
            try:
                cached_items = cache.get_multi([item["id"] for item in items])
            except pylibmc.Error as error:
                log.exception(error)
                cached_items = []
                pass
        to_ingest = []
        cached_items = list(cached_items.keys())
        for item in items:
            if item["id"] not in cached_items:
                to_ingest.append(item)
        return to_ingest

    def _get_modlog(self, admin, stream):
        subreddit = praw.Reddit(**self.reddit_params).subreddit(self.subreddit)
        params = {}
        if admin:
            params["mod"] = "a"
        if stream:
            params["pause_after"] = 0
            modlog = subreddit.mod.stream.log
            # modlog = subreddit.mod.stream.log(**params)
        else:
            params["limit"] = None
            # modlog = ChunkGenerator(
            #     subreddit._reddit, API_PATH["about_log"].format(subreddit=subreddit), limit=None, params=params
            # )
            modlog = subreddit.mod.log
        return modlog(**params)

    def admin_backlog(self):
        admin = True
        stream = False
        modlog = self._get_modlog(admin, stream)
        self._stream(admin, modlog, stream)

    def admin_stream(self):
        admin = True
        stream = True
        modlog = self._get_modlog(admin, stream)
        self._stream(admin, modlog, stream)

    def backlog(self):
        admin = False
        stream = False
        modlog = self._get_modlog(admin, stream)
        self._stream(admin, modlog, stream)

    def stream(self):
        admin = False
        stream = True
        modlog = self._get_modlog(admin, stream)
        self._stream(admin, modlog, stream)


def main():
    subreddits = Subreddit.query.all()
    to_set = {}
    accounts = {}
    for subreddit in subreddits:
        accounts.setdefault(subreddit.modlog_account, [])
        accounts[subreddit.modlog_account].append(subreddit.name)
        subreddit_webhooks = Webhook.query.get(subreddit.name)
        if subreddit_webhooks:
            for webhook_type in ["admin_webhook", "alert_webhook"]:
                webhook = getattr(subreddit_webhooks, webhook_type)
                if webhook:
                    to_set[f"{subreddit.name}_{webhook_type}"] = webhook
    cache.set_multi(to_set)
    for redditor, subreddits in accounts.items():
        for chunk, subreddit_chunk in enumerate([subreddits[x : x + 50] for x in range(0, len(subreddits), 50)]):
            start_streaming("+".join(subreddit_chunk), redditor, chunk)
    if sys.platform != "darwin":
        subreddits = services.reddit("Lil_SpazJoekp").user.me().moderated()
        chunks = list(
            zip_longest(
                *[
                    reversed(chunk) if i % 2 == 0 else chunk
                    for i, chunk in enumerate([subreddits[x : x + 25] for x in range(0, len(subreddits), 25)])
                ]
            )
        )
        for chunk, subreddit_chunk in enumerate(chunks):
            start_streaming("+".join([sub.display_name for sub in subreddit_chunk if sub]), "Lil_SpazJoekp", chunk)


def start_streaming(subreddit, redditor, chunk):
    try:
        log.info(f"Building chunk {chunk} for r/{subreddit} using u/{redditor}...")
        reddit = services.reddit(redditor)
        reddit_params = reddit.config._settings
        subreddit_streams = ModLogStreams(reddit_params, subreddit)
        for stream in subreddit_streams.STREAMS:
            log.info(f"Starting {stream} for r/{subreddit}")
            process = Process(target=getattr(subreddit_streams, stream), daemon=True)
            process.start()
            log.info(f"Started {stream} for r/{subreddit} (PID: {process.pid})")
    except NotFound as error:
        log.exception(error)


def set_cache():
    log.info("Setting cache...")
    conn = connection_pool.getconn()
    sql = conn.cursor()
    limit = 100000
    log.info(f"Fetching last {limit:,} ids")
    sql.execute("SELECT id FROM mirror.modlog ORDER BY created_utc DESC LIMIT %s", (limit,))
    results = sql.fetchall()
    connection_pool.putconn(conn)
    log.info(f"Caching {len(results):,} ids")
    cache.add_multi({result.id: result.id for result in results})
    log.info("Cache set")


def set_webhooks():
    subreddit_webhooks = Webhook.query.all()
    to_set = {}
    for subreddit_webhook in subreddit_webhooks:
        if subreddit_webhook:
            for webhook_type in ["admin_webhook", "alert_webhook"]:
                webhook = getattr(subreddit_webhook, webhook_type)
                if webhook:
                    to_set[f"{subreddit_webhook.subreddit}_{webhook_type}"] = webhook
    try:
        cache.set_multi(to_set)
    except Exception:
        pass


if __name__ == "__main__":
    try:
        cache.flush_all()
        set_cache()
        set_webhooks()
        main()
        while True:
            set_webhooks()
            time.sleep(30)
    except Exception as error:
        log.exception(error)
    except KeyboardInterrupt:
        pass
