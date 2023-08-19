import json
from database.db import database,  users
import aio_pika


async def produce_message(body: dict) -> None:
    connection = await aio_pika.connect_robust(host="rabbitmq", port=5672)

    async with connection:
        routing_key = "message_queue"

        channel = await connection.channel()
        query = users.select().where(users.c.is_blocked == False, users.c.chat_id != body["tg_user_or_chat"])
        live_users = await database.fetch_all(query=query)
        for i in live_users:
            body.update({"from_or_to": str(i.chat_id)})
            body.update({"is_blocked": i.is_blocked})
            body.update({"size": len(live_users)})
            message = aio_pika.Message(body=json.dumps(body).encode())
            await channel.default_exchange.publish(
                message=message, routing_key=routing_key)

