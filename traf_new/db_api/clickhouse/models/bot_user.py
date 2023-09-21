import asyncio
import datetime
from typing import Type, List

from clickhouse_sqlalchemy import engines, types
from sqlalchemy import Column, text, Integer, BigInteger

from traf_new.db_api.base import Database
from traf_new.db_api.clickhouse.clickhouse_base_dao import ClickHouseBaseDAO
from traf_new.db_api.clickhouse.enums.sex import Sex


class BotUser(Database.ClickHouseBase):
    __tablename__ = 'bot_user'

    __table_args__ = (
        engines.ReplacingMergeTree(
            primary_key=['bot_id', 'user_id'],
            order_by=['bot_id', 'user_id'],
            version='version'
            #partition_by='bot_id % 10'
        ),
    )

    bot_id = Column(types.UInt64, primary_key=True)
    user_id = Column(types.UInt64, primary_key=True)
    name = Column(types.String, nullable=False)
    fullname = Column(types.String, nullable=False)
    username = Column(types.Nullable(types.String), nullable=True)
    language = Column(types.Nullable(types.String), nullable=True)
    sex = Column(types.Nullable(types.Enum8(Sex)), nullable=True)
    chat_id = Column(types.Nullable(types.UInt64), nullable=True)
    alive = Column(types.UInt8, nullable=False)
    version = Column(types.UInt64, nullable=False)
    created = Column(types.DateTime, nullable=False)


class BotUserDAO(ClickHouseBaseDAO):
    _model: Type = BotUser
    _queue: asyncio.Queue = asyncio.Queue()

    @classmethod
    def get_by_bots(cls, bots: List[int]):
        db = Database.get_instance()

        with db.ClickHouseSession() as session:
            query = text('''
                                    SELECT bot_id, user_id, name, fullname, username, language, sex, chat_id, alive, version, created
                                    FROM bot_user
                                    WHERE bot_id in :bots
                                ''')

            results = session.execute(query, dict(bots=bots))
            if results:
                result = results.fetchall()
                if not result:
                    result = []
            else:
                result = []

        return result