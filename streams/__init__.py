import logging
from datetime import datetime, timezone

from BotUtils import BotServices
from celery import Task
from celery.signals import after_setup_logger, after_setup_task_logger
from pylibmc import Client
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from .logger import CeleryFormatter
from .utils import convert_or_none

thingTypes = {"t1": "Comment", "t2": "Account", "t3": "Link", "t4": "Message", "t5": "Subreddit", "t6": "Award"}
mapping = {
    "id": convert_or_none(lambda action_id: action_id.split("_")[1]),
    "created_utc": convert_or_none(lambda created_utc: datetime.fromtimestamp(created_utc).astimezone(timezone.utc)),
    "target_fullname": {
        "target_type": convert_or_none(lambda target_fullname: thingTypes[target_fullname.split("_")[0]]),
        "target_id": convert_or_none(lambda target_fullname: target_fullname.split("_")[1]),
    },
    "_mod": "moderator",
    "action": "mod_action",
}
skip_keys = ["mod_id36", "subreddit_name_prefixed", "sr_id36", "_reddit"]

services = BotServices("RedditModHelper")
log = services.logger()


@after_setup_logger.connect
def after_setup_logger(**kwargs):
    logger = logging.getLogger("celery.worker.strategy")
    logger.level = logging.DEBUG


@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    logColors = {
        "DEBUG": "bold_cyan",
        "INFO": "bold_green",
        "WARNING": "bold_yellow",
        "ERROR": "bold_red",
        "CRITICAL": "bold_purple",
    }
    secondaryLogColors = {
        "message": {
            "DEBUG": "bold_cyan",
            "INFO": "white",
            "WARNING": "bold_yellow",
            "ERROR": "bold_red",
            "CRITICAL": "bold_purple",
        }
    }
    colors = {"log_colors": logColors, "secondary_log_colors": secondaryLogColors}
    formatter = CeleryFormatter(
        "{asctime} [{log_color}{levelname:^9}{reset}] [{blue}{task_name}{reset}::{blue}{task_id}{reset}] [{cyan}{name}{reset}] {message_log_color}{message}",
        datefmt="%m/%d/%Y %I:%M:%S %p",
        style="{",
        **colors,
    )
    logger.addHandler(services.logger().handlers[0])
    for handler in logger.handlers:
        handler.setFormatter(formatter)


cache = Client(["localhost"])

params = services._getDbConnectionSettings()
url = f"postgresql://{params['user']}:{params['password']}@{params['host']}:{params['port']}/{params['database']}"
engine = create_engine(url)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

log_params = services._getDbConnectionSettings("RedditModHelperLogDB")
log_url = f"postgresql://{log_params['user']}:{log_params['password']}@{log_params['host']}:{log_params['port']}/{log_params['database']}"
log_engine = create_engine(log_url)
log_session_factory = sessionmaker(bind=log_engine)
LogSession = scoped_session(log_session_factory)


class DBTask(Task):
    _session: Session = None
    session_class = Session

    def after_return(self, *args, **kwargs):
        if self._session is not None:
            self._session.commit()
            self._session.close()

    @property
    def session(self) -> Session:
        if self._session is None:
            self._session = self.session_class()
        return self._session


class LogDBTask(DBTask):
    _session = None
    session_class = LogSession
