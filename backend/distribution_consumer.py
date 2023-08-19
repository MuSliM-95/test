import asyncio

from const import distribution_queue_name
from database.db import database, messages, users
import aio_pika
import json
import logging

from functions.goods_distribution import distribute


async def distribution_consumer() -> None:
    logging.basicConfig(level=logging.INFO)
    connection = await aio_pika.connect_robust(host="rabbitmq", port=5672, timeout=10)
    queue_name = distribution_queue_name
    await database.connect()
    async with connection:
        # Creating channel
        channel = await connection.channel()

        # Will take no more than 100 messages in advance
        await channel.set_qos(prefetch_count=100)

        # Declaring queue
        queue = await channel.declare_queue(queue_name)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                data = json.loads(message.body.decode())
                await distribute(**data)


if __name__ == "__main__":
    asyncio.run(distribution_consumer())
