import asyncio
import os

from apps.booking.repeat.worker.impl.RepeatBookingWorker import RepeatBookingWorker
from common.amqp_messaging.impl.RabbitFactory import RabbitFactory
from common.amqp_messaging.models.RabbitMqSettings import RabbitMqSettings
from database.db import database


async def startup():
    await database.connect()
    rabbit_factory = RabbitFactory(settings=RabbitMqSettings(
        rabbitmq_host=os.getenv('RABBITMQ_HOST'),
        rabbitmq_user=os.getenv('RABBITMQ_USER'),
        rabbitmq_pass=os.getenv('RABBITMQ_PASS'),
        rabbitmq_port=os.getenv('RABBITMQ_PORT'),
        rabbitmq_vhost=os.getenv('RABBITMQ_VHOST')
    ))

    repeat_booking_worker = RepeatBookingWorker(rabbitmq_messaging_factory=await rabbit_factory())
    await repeat_booking_worker.start()



if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.get_event_loop().run_until_complete(
        startup()
    )
