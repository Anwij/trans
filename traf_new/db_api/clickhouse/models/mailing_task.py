import asyncio
from typing import Type

from clickhouse_sqlalchemy import engines, types
from sqlalchemy import Column

from traf_new.db_api.base import Database
from traf_new.db_api.clickhouse.clickhouse_base_dao import ClickHouseBaseDAO
from traf_new.db_api.clickhouse.enums.mailing_task_type import MailingTaskType


class MailingTask(Database.ClickHouseBase):
    __tablename__ = 'mailing_task'

    __table_args__ = (
        engines.CollapsingMergeTree(
            primary_key=['bot_id', 'user_id', 'chat_id', 'msg_id', 'task_type', 'created'],
            sign_col='sign'
        ),
    )

    bot_id = Column(types.UInt64, primary_key=True)
    chat_id = Column(types.UInt64, nullable=False, default=0)
    user_id = Column(types.UInt64, primary_key=True)
    msg_id = Column(types.UInt64, nullable=False, default=0)
    sign = Column(types.Int8, nullable=False, default=1)
    task_type = Column(types.Enum8(MailingTaskType), primary_key=True)
    name = Column(types.Nullable(types.String), nullable=True)
    fullname = Column(types.Nullable(types.String), nullable=True)
    username = Column(types.Nullable(types.String), nullable=True)

    start_at = Column(types.DateTime, nullable=False)
    created = Column(types.DateTime, primary_key=True)


class MailingTaskDAO(ClickHouseBaseDAO):
    _model: Type = MailingTask
    _queue: asyncio.Queue = asyncio.Queue()