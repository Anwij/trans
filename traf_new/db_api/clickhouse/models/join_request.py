import asyncio
from typing import Type

from clickhouse_sqlalchemy import engines, types
from sqlalchemy import Column, text, func

from traf_new.db_api.base import Database
from traf_new.db_api.clickhouse.clickhouse_base_dao import ClickHouseBaseDAO, reconnect


class JoinRequest(Database.ClickHouseBase):
    __tablename__ = 'join_request'

    bot_id = Column(types.UInt64, primary_key=True)
    subscriber_id = Column(types.UInt64, primary_key=True)
    chat_id = Column(types.UInt64, primary_key=True)
    created = Column(types.DateTime, nullable=False)

    __table_args__ = (
        engines.ReplacingMergeTree(
            primary_key=['bot_id', 'chat_id', 'subscriber_id'],
            order_by=['bot_id', 'chat_id', 'subscriber_id'],
            version='created',
            ttl=created + func.toIntervalDay(30)
        ),
    )


class JoinRequestDAO(ClickHouseBaseDAO):
    _model: Type = JoinRequest
    _queue: asyncio.Queue = asyncio.Queue()

    @classmethod
    @reconnect
    def get_by_bot(cls, bot_id: int, limit: int):
        db = Database.get_instance()

        with db.ClickHouseSession() as session:
            if limit:
                query = text('''
                                SELECT bot_id, subscriber_id, chat_id, created
                                FROM join_request
                                WHERE bot_id = :bot_id
                                ORDER BY created
                                LIMIT :limit
                ''')
                results = session.execute(query, dict(bot_id=bot_id, limit=limit))
            else:
                query = text('''
                    SELECT bot_id, subscriber_id, chat_id, created
                    FROM join_request
                    WHERE bot_id = :bot_id
                    ORDER BY created
                ''')
                results = session.execute(query, dict(bot_id=bot_id))

            if results:
                result = [cls._model(**dict(zip(row.keys(), row))) for row in results.fetchall()]
                if not result:
                    result = []
            else:
                result = []

        return result

    @classmethod
    @reconnect
    def delete_by_bot(cls, bot_id: int, limit: int):
        db = Database.get_instance()

        with db.ClickHouseSession() as session:
            if limit:
                query = text('''
                                    ALTER TABLE join_request DELETE WHERE (bot_id, subscriber_id, chat_id) in (
                                        SELECT bot_id, subscriber_id, chat_id
                                        FROM join_request
                                        WHERE bot_id = :bot_id
                                        ORDER BY created
                                        LIMIT :limit
                                    )
                    ''')
                session.execute(query, dict(bot_id=bot_id, limit=limit))
            else:
                query = text('''
                                                    ALTER TABLE join_request DELETE WHERE (bot_id, subscriber_id, chat_id) in (
                                                        SELECT bot_id, subscriber_id, chat_id
                                                        FROM join_request
                                                        WHERE bot_id = :bot_id
                                                        ORDER BY created
                                                    )
                                    ''')
                session.execute(query, dict(bot_id=bot_id))

            session.commit()