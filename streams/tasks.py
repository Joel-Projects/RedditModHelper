from functools import partial

from celery import Celery
from discord import RequestsWebhookAdapter, Webhook

from . import LogDBTask, cache, log, mapping, models, skip_keys
from .models import ModlogInsert
from .utils import gen_action_embed, map_values

Webhook = partial(Webhook.from_url, adapter=RequestsWebhookAdapter())

app = Celery(
    "streams",
    broker="redis://localhost/1",
    task_cls="streams:DBTask",
    accept_content=["pickle"],
    result_serializer="pickle",
    task_serializer="pickle",
)
app.config_from_object("streams.celery_config")


@app.task(bind=True, task_cls=LogDBTask, ignore_result=True)
def ingest_action(self, data, admin, is_stream):
    try:
        modlog_item = ModlogInsert(**data)
        self.session.add(modlog_item)
        try:
            self.session.commit()
        except Exception as error:
            log.exception(error)
            self.session.rollback()
            self.retry()
        finally:
            cache.add(data["id"], data["id"])
        new = modlog_item.query_action == "insert"

        status = "New" if new else "Old"
        if not is_stream:
            status = f"Past {status.lower()}"
        getattr(log, "info" if status in ["New", "Past new"] else "debug")(
            f"{status}{' | admin' if admin else ''} | {data['subreddit']} | {data['moderator']} | {data['mod_action']} | {data['created_utc'].strftime('%m-%d-%Y %I:%M:%S %p')}"
        )
        if admin and is_stream and (modlog_item.query_action if modlog_item else "update") == "insert":
            webhook = models.Webhook.query.get(data["subreddit"])
            if webhook and webhook.admin_webhook:
                result = send_admin_alert.delay(data, webhook.admin_webhook)
                result.forget()
    except Exception as error:
        log.exception(error)
        self.retry()


@app.task(ignore_result=True)
def send_admin_alert(action, webhook):
    webhook = Webhook(webhook)
    embed, get_more = gen_action_embed(action)
    webhook.send(
        f"To see the entire body run this command:\n`.getbody https://reddit.com{action['target_permalink']}`"
        if get_more
        else None,
        embed=embed,
    )
    log.info(f"Notifying r/{action['subreddit']} of admin action by u/{action['moderator']}")
