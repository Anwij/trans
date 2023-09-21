import asyncio
import time
import traceback

import clickhouse_sqlalchemy
import logging

from clickhouse_sqlalchemy.exceptions import DatabaseException


def reconnect(func):
    def database_method(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except DatabaseException as error:
                if 'Code: 210. Connection refused' in str(error) or 'Code: 209' in str(error):
                    time.sleep(1)
                    continue
                else:
                    logging.warning(''.join(traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__)))
                    raise error
            except Exception as error:
                logging.warning(''.join(traceback.format_exception(etype=type(error), value=error, tb=error.__traceback__)))
                raise error
    return database_method