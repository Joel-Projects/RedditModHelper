from functools import partial

from celery import Celery
from discord import RequestsWebhookAdapter, Webhook
from kombu import Exchange, Queue
from psycopg2.extras import execute_values

from . import cache, log, models
from .utils import gen_action_embed

Webhook = partial(Webhook.from_url, adapter=RequestsWebhookAdapter())

app = Celery(
    "streams",
    broker="amqp://guest:guest@localhost:5672/RedditModHelper",
    task_default_priority=0,
    task_cls="streams:DBTask",
    accept_content=["pickle"],
    result_serializer="pickle",
    task_serializer="pickle",
    # task_routes=["streams.routers.route_task"],
)
app.config_from_object("streams.celery_config")
default_exchange = Exchange("default", type="direct")
mod_log_exchange = Exchange("mod_log", type="direct")
alert_exchange = Exchange("alerts", type="direct")

app.conf.task_queues = [
    Queue("default", default_exchange, routing_key="default"),
    Queue("admin_alerts", alert_exchange, routing_key="alerts.admin"),
    Queue(
        "actions", mod_log_exchange, routing_key="mod_log.actions", queue_arguments={"x-max-priority": 2}, durable=False
    ),
    Queue(
        "action_chunks",
        mod_log_exchange,
        routing_key="mod_log.action_chunks",
        queue_arguments={"x-max-priority": 2},
        durable=False,
    ),
]
app.conf.task_default_queue = "default"
app.conf.task_default_exchange = "default"
app.conf.task_default_routing_key = "default"

QUERY = """INSERT INTO mirror.modlog(id, created_utc, moderator, subreddit, mod_action, details, description, target_author, target_body, target_type, target_id, target_permalink, target_title, pinged, query_action)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, false, 'insert')
           ON CONFLICT (id, created_utc) DO UPDATE SET query_action='updated'
           RETURNING (query_action = 'insert') as new;
           """
CHUNK_QUERY = """INSERT INTO mirror.modlog(id, created_utc, moderator, subreddit, mod_action, details, description, target_author, target_body, target_type, target_id, target_permalink, target_title, pinged, query_action)
                 VALUES %s
                 ON CONFLICT (id, created_utc) DO UPDATE SET query_action='updated' RETURNING (query_action='insert') as new;
                 """


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
        new = cache.get(data["id"]) != 1
        if new:
            with self.pool as sql:
                try:
                    sql.execute(QUERY, [data.get(key, None) for key in columns])
                    modlog_item = sql.fetchone()
                    new = modlog_item.new
                    cache.add(data["id"], 1)
                except Exception as error:
                    log.exception(error)
                    self.retry()

        status = "New" if new else "Old"
        if not is_stream:
            status = f"Past {status.lower()}"
        getattr(log, "info" if status in ["New", "Past new"] else "debug")(
            f"{status}{' | admin' if admin else ''} | {data['subreddit']} | {data['moderator']} | {data['mod_action']} | {data['created_utc'].astimezone().strftime('%m-%d-%Y %I:%M:%S %p')}"
        )
        if admin:
            check_admin(self, data)
    except Exception as error:
        log.exception(error)
        self.retry()


@app.task(bind=True, ignore_result=True)
def ingest_action_chunk(self, actions):
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
        results = []
        with self.pool as sql:
            try:
                results = execute_values(
                    sql,
                    CHUNK_QUERY,
                    [tuple([data.get(key, None) for key in columns] + [False, "insert"]) for data, _, _ in actions],
                    fetch=True,
                )
            except Exception as error:
                log.exception(error)
                self.retry()
        cache.add_multi({data["id"]: 1 for data, _, _ in actions})
        for i, modlog_item in enumerate(results):
            new = modlog_item.new
            data, admin, is_stream = actions[i]
            status = "New" if new else "Old"
            if not is_stream:
                status = f"Past {status.lower()}"
            getattr(log, "info" if new else "debug")(
                f"{status}{' | admin' if admin else ''} | {data['subreddit']} | {data['moderator']} | {data['mod_action']} | {data['created_utc'].astimezone().strftime('%m-%d-%Y %I:%M:%S %p')}"
            )
            if admin:
                check_admin(self, data)
    except Exception as error:
        log.exception(error)
        self.retry()


def check_admin(self, data):
    with self.pool as sql:
        sql.execute("SELECT pinged FROM mirror.modlog WHERE id=%s", (data["id"],))
        modlog_item = sql.fetchone()
        if modlog_item:
            pinged = modlog_item.pinged
            if not pinged:
                webhook = cache.get(f"{data['subreddit']}_admin_webhook")
                if not webhook:
                    subreddit = models.Webhook.query.get(data["subreddit"])
                    if subreddit:
                        webhook = subreddit.admin_webhook
                if webhook:
                    send_admin_alert.apply_async(args=[data, webhook], queue="admin_alerts")
                    sql.execute("UPDATE mirror.modlog SET pinged=true WHERE id=%s", (data["id"],))
        else:
            self.retry()


@app.task(ignore_result=True)
def send_admin_alert(action, webhook):
    embed = None
    try:
        webhook = Webhook(webhook)
        embed, get_more = gen_action_embed(action)
        webhook.send(  # f"To see the entire body run this command:\n`.getbody https://reddit.com{action['target_permalink']}`"
            # if get_more
            # else None,
            None,
            embed=embed,
        )
        log.info(f"Notifying r/{action['subreddit']} of admin action by u/{action['moderator']}")
    except Exception as error:
        log.exception(error)
        if embed:
            log.info(embed.to_dict())


if __name__ == "__main__":
    app.start()
