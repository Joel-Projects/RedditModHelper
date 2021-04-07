import os
import time
from functools import partial

from celery import Celery
from discord import RequestsWebhookAdapter, Webhook

from . import cache, log, models
from .utils import gen_action_embed
from kombu import Exchange, Queue

Webhook = partial(Webhook.from_url, adapter=RequestsWebhookAdapter())

app = Celery(
    "streams",
    broker="amqp://guest:guest@localhost:5672/RedditModHelper",
    task_default_priority=0,
    task_cls="streams:DBTask",
    accept_content=["pickle"],
    result_serializer="pickle",
    task_serializer="pickle",
    task_routes = ['streams.routers.route_task']
)
app.config_from_object("streams.celery_config")
default_exchange = Exchange('default', type='direct')
mod_log_exchange = Exchange('mod_log', type='direct')

app.conf.task_queues = [
    Queue('default', default_exchange, routing_key='default'),
    Queue('actions', mod_log_exchange, routing_key='mod_log.actions', queue_arguments={'x-max-priority': 4}),
]
app.conf.task_default_queue = 'default'
app.conf.task_default_exchange = 'default'
app.conf.task_default_routing_key = 'default'

QUERY = "INSERT INTO mirror.modlog_insert(id, created_utc, moderator, subreddit, mod_action, details,  description, target_author, target_body, target_type, target_id, target_permalink, target_title, query_action) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'insert') RETURNING (query_action='insert') as new;"


@app.task(bind=True, ignore_result=True)
def task_test(self, level):
    print('Received: ', level, self.request.delivery_info['priority'])


@app.task(bind=True, ignore_result=True)
def ingest_action(self, data, admin, is_stream):
    try:
        columns = [
            "id",
            "created_utc",
            "moderator",
            "subreddit",
            "mod_action",
            "details",
            "description",
            "target_author",
            "target_body",
            "target_type",
            "target_id",
            "target_permalink",
            "target_title",
        ]
        new = True
        try:
            conn = self._pool.getconn()
            sql = conn.cursor()
            sql.execute(QUERY, [data.get(key, None) for key in columns])
            modlog_item = sql.fetchone()
            new = modlog_item.new
            cache.add(data["id"], data["id"])
            self._pool.putconn(conn)
        except Exception as error:
            log.exception(error)
            self.retry()

        status = "New" if new else "Old"
        if not is_stream:
            status = f"Past {status.lower()}"

        getattr(log, "info" if status in ["New", "Past new"] else "debug")(
            f"{status}{' | admin' if admin else ''} | {data['subreddit']} | {data['moderator']} | {data['mod_action']} | {data['created_utc'].astimezone().strftime('%m-%d-%Y %I:%M:%S %p')}"
        )
        if admin and is_stream and new:
            webhook = cache.get(f"{data['subreddit']}_admin_webhook")
            if not webhook:
                subreddit = models.Webhook.query.get(data["subreddit"])
                if subreddit:
                    webhook = subreddit.admin_webhook
            if webhook:
                result = send_admin_alert.delay(data, webhook)
                result.forget()
    except Exception as error:
        log.exception(error)
        self.retry()

# TODO separate out stream and backlog

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
