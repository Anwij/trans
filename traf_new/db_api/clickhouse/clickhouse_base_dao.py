import asyncio
import json
import logging
import traceback
from asyncio import get_event_loop, new_event_loop
from multiprocessing.pool import ThreadPool
from time import time
from typing import Optional, List, Union, Any

import clickhouse_sqlalchemy
from clickhouse_sqlalchemy import select
from sqlalchemy import text
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy.sql import functions

from traf_new.db_api.base import Database
from traf_new.db_api.clickhouse.reconnector import reconnect


class ClickHouseBaseDAO:
    _model: DeclarativeMeta = None
    _queue: asyncio.Queue = None

    @classmethod
    async def put(cls, obj: object):
        return await cls._queue.put(obj)

    @classmethod
    @reconnect
    def get(
            cls,
            id: Optional[int] = None,
            many: bool = False,
            *args,
            **kwargs
    ) -> Union[Optional[Any], List[Any]]:
        db = Database.get_instance()

        filters = []

        if id is not None:
            filters.append(getattr(cls._model, 'id') == id)
        else:
            for key, value in kwargs.items():
                filters.append(getattr(cls._model, key) == value)

        if kwargs and many:
            raise Exception("You can't use 'get many' with parameters")

        if many:
            query = text(f'SELECT * FROM {cls._model.__table__}')
        else:
            query = select(cls._model).where(*filters)

        with db.ClickHouseSession() as session:
            results = session.execute(query)

            if many:
                result = [cls._model(**dict(zip(row.keys(), row))) for row in results.fetchall()]
            else:
                result = results.fetchone()
                if result:
                    (result,) = result

        filters.clear()

        return result

    @classmethod
    @reconnect
    def add(cls, rows):
        db = Database.get_instance()
        with db.ClickHouseSession() as session:
            try:
                session.add_all(rows)
                session.commit()
            except Exception as error:
                if len(rows) > 1:
                    for row in rows:
                        cls.add([row])
                else:
                    text = '\n'.join([str(row.__dict__) for row in rows])
                    logging.critical(text)
        rows.clear()

    @classmethod
    @reconnect
    def count(cls, *args, **kwargs) -> int:
        db = Database.get_instance()

        filters = []

        for key, value in kwargs.items():
            filters.append(getattr(cls._model, key) == value)

        query = select(functions.count()).select_from(cls._model).where(*filters)

        filters.clear()

        with db.ClickHouseSession() as session:
            results = session.execute(query)
            (result,) = results.fetchone()

        return result

    @classmethod
    @reconnect
    def drop(cls):
        cls._model.__table__.drop(checkfirst=True, if_exists=True)

    @classmethod
    @reconnect
    def create(cls):
        cls._model.__table__.create(checkfirst=True)

    @classmethod
    def exists(cls, id: Optional[int] = None, *args, **kwargs) -> bool:
        if id is not None:
            kwargs['id'] = id
        return cls.count(*args, **kwargs) > 0

    @classmethod
    @reconnect
    def optimize(cls):
        db = Database.get_instance()

        with db.ClickHouseSession() as session:
            query = text(f'''OPTIMIZE TABLE {cls._model.__table__} FINAL''')

            session.execute(query)
