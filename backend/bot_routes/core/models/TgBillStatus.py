from enum import Enum


class TgBillStatus(str, Enum):
    NEW = "new"
    WAITING_FOR_APPROVAL = "waiting_for_approval"
    APPROVED = "approved"
    CANCELED = "canceled"
    REJECTED = "rejected"
    REQUESTED = "requested"
    PAID = "paid"
    ERROR = "error"
