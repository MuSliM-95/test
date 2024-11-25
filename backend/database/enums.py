from enum import Enum


class Repeatability(str, Enum):
    minutes = "minutes"
    hours = "hours"
    days = "days"
    weeks = "weeks"
    months = "months"


class Gender(str, Enum):
    male = "Мужчина"
    female = "Женщина"


class ContragentType(str, Enum):
    company = "Компания"
    contact = "Контакт"
