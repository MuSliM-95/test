import re

from typing import Optional

from pydantic import BaseModel, validator, constr

HEXColor = constr(regex=r"^#(?:[0-9a-fA-F]{6})$")


class TagCreate(BaseModel):
    name: str
    color: Optional[HEXColor]
    emoji: Optional[str]
    description: Optional[str]

class TagDelete(TagCreate):
    pass


class Tag(BaseModel):
    id: int
    name: str
    color: Optional[HEXColor]
    emoji: Optional[str]
    description: Optional[str]
