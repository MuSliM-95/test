from enum import Enum


class Repeatability(str, Enum):
    minutes = "minutes"
    hours = "hours"
    days = "days"
    weeks = "weeks"
    months = "months"
