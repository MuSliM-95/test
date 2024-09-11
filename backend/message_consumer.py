import asyncio
import os

import aio_pika
import json
import logging

from database.db import database, messages, users
from bot import finish_broadcast_messaging, message_to_chat_by_id

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Message-Consumer")
logger.setLevel(logging.INFO)


async def message_consumer() -> None:
    logger.info("START CONSUMER")
    connection = await aio_pika.connect_robust(
        host=os.getenv('RABBITMQ_HOST'),
        port=os.getenv('RABBITMQ_PORT'),
        login=os.getenv('RABBITMQ_USER'),
        password=os.getenv('RABBITMQ_PASS'),
        virtualhost=os.getenv('RABBITMQ_VHOST'),
        timeout=10)
    await database.connect()
    async with connection:
        channel = await connection.channel()

        # await channel.set_qos(prefetch_count=100)

        queue = await channel.declare_queue("message_queue")

        query = users.select(users.c.is_blocked == True)
        s = await database.fetch_all(query)
        active_before = len(s)
        message_count = 0
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                message_count += 1
                data = json.loads(message.body.decode())
                if data["is_blocked"] == True:
                    active_before += 1
                chat_id = data['tg_user_or_chat']
                relship = messages.insert().values(
                    tg_message_id=data['message_id'] + message_count + 1,
                    tg_user_or_chat=data['tg_user_or_chat'],
                    from_or_to=str(data['from_or_to']),
                    created_at=data['created_at'],
                    body=data['text']
                )

                await message_to_chat_by_id(chat_id=str(data['from_or_to']), message=data['text'],
                                            picture=data['picture'])
                await message.ack()  # Accept message
                await database.execute(relship)
                if message_count == data['size']:
                    query = users.select(users.c.is_blocked == True)
                    s = await database.fetch_all(query)
                    active_after = len(s)
                    await finish_broadcast_messaging(chat_id, data['size'], active_before,
                                                     active_after, data['message_id'] + message_count + 1)
                    await channel.queue_delete(queue_name='message_queue')
                    await message_consumer()


if __name__ == "__main__":
    asyncio.run(message_consumer())
