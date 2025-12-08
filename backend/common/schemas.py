from typing import Annotated, Union
import phonenumbers
from pydantic_extra_types import phone_numbers

RuPhone = Annotated[
    Union[str, phonenumbers.PhoneNumber],
    phone_numbers.PhoneNumberValidator(supported_regions=['RU'], default_region='RU', number_format="E164")
]