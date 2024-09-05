from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class IRabbitMessaging:

    async def publish(
        self,
        message: BaseModelMessage,
        routing_key: str
    ):
        raise NotImplementedError()

    async def subscribe(self):
        raise NotImplementedError()

