from enum import Enum


class TgBillApproveStatus(str, Enum):
    NEW = "new"
    APPROVED = "approved"
    CANCELED = "canceled"