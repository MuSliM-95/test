from apps.amocrm.leads.handlers.impl.PostLeadEvent import PostLeadEvent
from apps.amocrm.leads.models.NewLeadBaseModelMessage import NewLeadBaseModelMessage
from apps.amocrm.leads.repositories.impl.LeadsRepository import LeadsRepository
from common.amqp_messaging.common.core.IRabbitFactory import IRabbitFactory
from common.amqp_messaging.common.core.IRabbitMessaging import IRabbitMessaging


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

        post_lead_handler = PostLeadEvent(
            leads_repository=LeadsRepository()
        )

        await rabbitmq_messaging.subscribe(NewLeadBaseModelMessage, post_lead_handler)

        await rabbitmq_messaging.install(queue_name="post_amo_lead")
