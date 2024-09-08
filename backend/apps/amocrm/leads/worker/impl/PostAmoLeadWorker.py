from json import loads

from pydantic import ValidationError

from apps.amocrm.leads.handlers.impl.PostLeadEvent import PostLeadEvent
from apps.amocrm.leads.models.NewLeadBaseModelMessage import NewLeadBaseModelMessage
from apps.amocrm.leads.repositories.impl.LeadsRepository import LeadsRepository
from common.amqp_messaging.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.core.IRabbitMessaging import IRabbitMessaging


class PostAmpLeadWorker:

    def __init__(
        self,
        rabbitmq_messaging_factory: IRabbitFactory
    ):
        self.__rabbitmq_messaging_factory = rabbitmq_messaging_factory

    async def start(
        self
    ):
        rabbitmq_messaging: IRabbitMessaging = await self.__rabbitmq_messaging_factory()
        async for data_bytes in rabbitmq_messaging.subscribe(queue_name="post_amo_lead"):
            message_json = loads(data_bytes)
            try:
                validated_message = NewLeadBaseModelMessage(**message_json)
                post_lead_event = PostLeadEvent(
                    post_amo_lead_message=validated_message,
                    leads_repository=LeadsRepository()
                )
                await post_lead_event()
            except ValidationError:
                print("Ошибка валидации")