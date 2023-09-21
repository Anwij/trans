import datetime
import re
import traceback
from asyncio import get_event_loop

import traf_new
from traf.db_api.base import Database

from traf.db_api.clickhouse.models.bot_user import BotUserDAO
from traf_new.db_api.clickhouse.models import bot_user
from traf_new.db_api.clickhouse.models.bot_user import BotUser

from traf.db_api.clickhouse.models.join_request import JoinRequestDAO
from traf_new.db_api.clickhouse.models import join_request
from traf_new.db_api.clickhouse.models.join_request import JoinRequest

from traf.db_api.clickhouse.models.mailing_task import MailingTaskDAO
from traf_new.db_api.clickhouse.models import mailing_task
from traf_new.db_api.clickhouse.models.mailing_task import MailingTask


async def run():
    db = Database.get_instance()
    await db.create_all()

    db2 = traf_new.db_api.base.Database.get_instance()
    await db2.create_all()
    # await transfer_admins()
    # await transfer_bots()
    # await transfer_messages()
    # await transfer_chats()
    # await transfer_chat_messages()
    # await transfer_mailings()
    await transfer_bot_users()
    await transfer_join_request()
    await transfer_mailing_tasks()


async def transfer_bot_users():
    for i in range(0, 14):
        bot_users = BotUserDAO.get_by_bots(i)
        print(len(bot_users))
        bs = []
        c = 0
        for b in bot_users:
            bs.append(BotUser(
                bot_id=b[0],
                user_id=b[1],
                name=b[2],
                fullname=b[3],
                username=b[4],
                language=b[5],
                sex=b[6],
                chat_id=b[7],
                alive=b[8],
                version=b[9],
                created=datetime.datetime.utcnow()
            ))
            if c % 20000 == 0 and c != 0:
                print(c, 100 * c / len(bot_users), 'bot users')
                bot_user.BotUserDAO.add(bs)
                bs.clear()
        
        bot_user.BotUserDAO.add(bs)
    

async def transfer_join_request():
    jrs = JoinRequestDAO.get(many=True)
    print(len(jrs))
    bs = []
    c = 0
    for jr in jrs:
        bs.append(JoinRequest(
            bot_id=jr.bot_id,
            subscriber_id=jr.subscriber_id,
            chat_id=jr.chat_id,
            created=jr.created
        ))
        if c % 20000 == 0 and c != 0:
            print(c, 100 * c / len(jrs), 'join requests')
            join_request.JoinRequestDAO.add(bs)
            bs.clear()
    
    join_request.JoinRequestDAO.add(bs)
    

async def transfer_mailing_tasks():
    mts = MailingTaskDAO.get(many=True)
    print(len(mts))
    bs = []
    c = 0
    for mt in mts:
        bs.append(MailingTask(
            bot_id=mt.bot_id,
            chat_id=mt.chat_id,
            user_id=mt.user_id,
            msg_id=mt.msg_id,
            sign=mt.sign,
            task_type=mt.task_type,
            name=mt.name,
            fullname=mt.fullname,
            username=mt.username,
            start_at=mt.start_at,
            created=mt.created,
        ))
        if c % 20000 == 0 and c != 0:
            print(c, 100 * c / len(mts), 'mailing tasks')
            mailing_task.MailingTaskDAO.add(bs)
            bs.clear()
    
    mailing_task.MailingTaskDAO.add(bs)


get_event_loop().run_until_complete(run())