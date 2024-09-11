import asyncio
import os

from apps.amocrm.leads.worker.impl.PostAmoLeadWorker import PostAmpLeadWorker
from common.amqp_messaging.common.impl.RabbitFactory import RabbitFactory
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
    post_amo_lead_worker = PostAmpLeadWorker(rabbitmq_messaging_factory=await rabbit_factory())
    await post_amo_lead_worker.start()



if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.get_event_loop().run_until_complete(
        startup()
    )
