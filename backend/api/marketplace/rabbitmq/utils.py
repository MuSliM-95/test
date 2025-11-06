import os

from common.amqp_messaging.common.impl.RabbitFactory import RabbitFactory
from common.amqp_messaging.models.RabbitMqSettings import RabbitMqSettings


def get_rabbitmq_factory():
    return RabbitFactory(settings=RabbitMqSettings(
        rabbitmq_host=os.getenv('RABBITMQ_HOST'),
        rabbitmq_user=os.getenv('RABBITMQ_USER'),
        rabbitmq_pass=os.getenv('RABBITMQ_PASS'),
        rabbitmq_port=os.getenv('RABBITMQ_PORT'),
        rabbitmq_vhost=os.getenv('RABBITMQ_VHOST')
    ))
