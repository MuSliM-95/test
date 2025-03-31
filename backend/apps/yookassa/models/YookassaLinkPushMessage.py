from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class YookassaLinkPushMessage(BaseModelMessage):
    amo_install_group_id: int
    cashbox_id: int