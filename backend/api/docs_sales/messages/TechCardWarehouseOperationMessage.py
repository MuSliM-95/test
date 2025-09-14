from typing import List

from uuid import UUID

from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class TechCardWarehouseOperationMessage(BaseModelMessage):
    tech_card_operation_uuid: UUID
    user_cashbox_id: int
