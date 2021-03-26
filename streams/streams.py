import time
from multiprocessing import Process

import praw
from BotUtils import BotServices
from credmgr.exceptions import NotFound

from streams.tasks import ingest_action
from streams.utils import map_values

from . import cache, log, mapping, services, skip_keys
from .models import Subreddit, Webhook

services = BotServices("SiouxBot")


class ModLogStreams:
    STREAMS = ["admin_backlog", "admin_stream", "backlog", "stream"]

    def __init__(self, reddit_params, subreddit):
        self.reddit_params = reddit_params
        self.subreddit = subreddit

    @staticmethod
    def _stream(admin, modlog, stream):
        while True:
            for action in modlog:
                try:
                    data = map_values(action.__dict__, mapping, skip_keys)
                    ingest_action.delay(action, admin, stream)
                    status = "New" if stream else "Old"
                    if not stream:
                        status = f"Past {status.lower}"
                    log.debug(
                        f"{status}{' | admin' if admin else ''} | {data['subreddit']} | {data['moderator']} | {data['mod_action']} | {data['created_utc'].strftime('%m-%d-%Y %I:%M:%S %p')}"
                    )
                except Exception as error:
                    log.exception(error)

    def _get_modlog(self, admin, backlog):
        subreddit = praw.Reddit(**self.reddit_params).subreddit(self.subreddit)
        params = {}
        if admin:
            params["mod"] = "a"
        if backlog:
            params["limit"] = None
            modlog = subreddit.mod.log
        else:
            modlog = subreddit.mod.stream.log
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
    started_subreddits = cache.get("subreddits") or []
    to_set = {}
    accounts = {}
    for subreddit in subreddits:
        if subreddit not in started_subreddits:
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


def start_streaming(subreddit, redditor, chunk):
    try:
        log.info(f"Building chunk {chunk} streams for r/{subreddit} using u/{redditor}...")
        reddit = services.reddit(redditor)
        reddit_params = reddit.config._settings
        subreddit_streams = ModLogStreams(reddit_params, subreddit)
        for stream in subreddit_streams.STREAMS:
            log.info(f"Starting {stream} stream for r/{subreddit}")
            process = Process(target=getattr(subreddit_streams, stream), daemon=True)
            process.start()
            log.info(f"Started {stream} stream for r/{subreddit} (PID: {process.pid})")
    except NotFound as error:
        log.exception(error)


if __name__ == "__main__":
    try:
        main()
        while True:
            time.sleep(500)
    except Exception as error:
        log.exception(error)
    except KeyboardInterrupt:
        pass
