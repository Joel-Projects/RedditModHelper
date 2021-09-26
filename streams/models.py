from sqlalchemy import CHAR, TIMESTAMP, BigInteger, Boolean, Column, String, Text
from sqlalchemy.ext.declarative import declarative_base

from . import Session, services

session = services.sqlalc()
Base = declarative_base(bind=session.bind)


LogBase = declarative_base()


class ModlogInsert(LogBase):
    __tablename__ = "modlog_insert"
    __table_args__ = {"schema": "mirror"}

    id = Column(CHAR(36), primary_key=True, nullable=False)
    created_utc = Column(TIMESTAMP(True), primary_key=True, nullable=False)
    moderator = Column(String(30), nullable=False)
    subreddit = Column(String(30), nullable=False)
    mod_action = Column(Text, nullable=False)
    details = Column(Text)
    description = Column(Text)
    target_author = Column(String(30))
    target_body = Column(Text)
    target_type = Column(Text)
    target_id = Column(Text)
    target_permalink = Column(Text)
    target_title = Column(Text)
    pinged = Column(Boolean)
    target_fullname = Column(Text)
    query_action = Column(Text)

    def __repr__(self):
        return f"<ModAction(subreddit='{self.subreddit}', mod='{self.moderator}', action='{self.mod_action}')>"


class Subreddit(Base):
    __tablename__ = "subreddits"
    __table_args__ = {"schema": "redditmodhelper"}

    name = Column(Text, primary_key=True)
    role_id = Column(BigInteger, nullable=False)
    channel_id = Column(BigInteger, nullable=False)
    modlog_account = Column(Text)
    alert_channel_id = Column(BigInteger)
    # backlogs_ingested = Column(Boolean)

    query = Session.query_property()

    def __repr__(self):
        return f"<Subreddit(subreddit='{self.name}', mod_account='{self.modlog_account}'>"


class Webhook(Base):
    __tablename__ = "webhooks"
    __table_args__ = {"schema": "redditmodhelper"}

    subreddit = Column(Text, primary_key=True)
    admin_webhook = Column(Text)
    alert_webhook = Column(Text)

    query = Session.query_property()

    def __repr__(self):
        return f"<Webhook(subreddit='{self.subreddit}'>"
