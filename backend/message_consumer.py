import asyncio
from database.db import database, messages, users
import aio_pika
import json
import logging


async def message_consumer() -> None:
    logging.basicConfig(level=logging.INFO)
    connection = await aio_pika.connect_robust(host="rabbitmq", port=5672, timeout=10)
    queue_name = "message_queue"
    await database.connect()
    async with connection:
        # Creating channel
        channel = await connection.channel()

        # Will take no more than 100 messages in advance
        await channel.set_qos(prefetch_count=100)

        # Declaring queue
        queue = await channel.declare_queue(queue_name)
        query = users.select(users.c.is_blocked == True)
        s = await database.fetch_all(query)
        print([i.id for i in s])
        active_before = len(s)
        print(f"Blocked before: {s}")
        message_count = int(0)
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
                from bot import finish_broadcast_messaging, message_to_chat_by_id
                await message_to_chat_by_id(chat_id=str(data['from_or_to']), message=data['text'])
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
