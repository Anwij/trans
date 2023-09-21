from typing import Optional

from clickhouse_sqlalchemy import get_declarative_base
from sqlalchemy import event, DDL, create_engine, MetaData, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base, DeclarativeMeta


class Database:

    _instance: Optional['Database'] = None
    _postgresql_url: Optional[str] = None
    _clickhouse_url: Optional[str] = None
    _postgresql_connections: Optional[int] = None
    _clickhouse_connections: Optional[int] = None

    def __init__(self):
        # self._postgresql_engine = create_async_engine(
        #     Database._postgresql_url,
        #     pool_size=self._postgresql_connections,
        #     max_overflow=10 * self._postgresql_connections
        # )
        # self._PostgresqlSession: sessionmaker = sessionmaker(self._postgresql_engine, expire_on_commit=False, class_=AsyncSession)
        # self._PostgresqlBase = declarative_base()

        self._clickhouse_engine = create_engine(Database._clickhouse_url, pool_size=self._clickhouse_connections)
        self._ClickHouseSession: sessionmaker = sessionmaker(self._clickhouse_engine, expire_on_commit=False)
        self._ClickHouseBase = get_declarative_base(metadata=MetaData(bind=self._clickhouse_engine))

    @classmethod
    def bind(cls, postgresql_url: str, clickhouse_url: str, postgresql_connections: int, clickhouse_connections: int):
        cls._postgresql_url = postgresql_url
        cls._clickhouse_url = clickhouse_url
        cls._postgresql_connections = postgresql_connections
        cls._clickhouse_connections = clickhouse_connections

    @classmethod
    def get_instance(cls) -> 'Database':
        if cls._instance is None:
            cls._instance = Database()
        return cls._instance

    @classmethod
    async def close(cls):
        await cls.get_instance()._postgresql_engine.dispose()
        cls.get_instance()._clickhouse_engine.dispose()

    @classmethod
    async def create_all(cls):

        instance = cls.get_instance()

        cls.init_clickhouse_models()

        for table in instance.ClickHouseBase.metadata.tables.values():
            table.create(checkfirst=True)

    @classmethod
    def init_clickhouse_models(cls):
        pass

    @classmethod
    def ping_clickhouse(cls):
        db = Database.get_instance()

        with db.ClickHouseSession() as session:
            query = text('''SELECT 1''')
            session.execute(query)

    @classmethod
    @property
    def ClickHouseSession(cls) -> sessionmaker:
        return cls.get_instance()._ClickHouseSession

    @classmethod
    @property
    def ClickHouseBase(cls) -> DeclarativeMeta:
        return cls.get_instance()._ClickHouseBase

    @classmethod
    @property
    def engine(cls):
        return cls.get_instance()._postgresql_engine


Database.bind('', 'clickhouse+native://tgreet:ds4teQFiquq8@10.0.1.12:9000/tgreet_db', 1, 1)