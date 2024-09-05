from common.amqp_messaging.models.BaseModelMessage import BaseModelMessage


class BaseBookingRepeatMessage(BaseModelMessage):
    cashbox_id: int
    booking_id: int
    start_booking: int
    end_booking: int