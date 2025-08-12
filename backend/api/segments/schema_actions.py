from typing import Optional, List, Literal

from pydantic import BaseModel, validator, constr

HEXColor = constr(regex=r"^#(?:[0-9a-fA-F]{6})$")

class AddRemoveTags(BaseModel):
    name: List[str]

    @validator("name", each_item=True)
    def validate_tag_item(cls, v):
        if len(v) < 3:
            raise ValueError("Элемент списка должен быть не короче 3 символов")
        return v


class Tag(BaseModel):
    name: str
    emoji: Optional[str]
    color: Optional[HEXColor]
    description: Optional[str]


class CreateTags(BaseModel):
    tags: List[Tag]


class TgNotificationsAction(BaseModel):
    trigger_on_new: bool = True
    message: str
    user_tag: str
    send_to: Optional[Literal["picker", "courier"]]


class DocsSalesTags(BaseModel):
    tags: List[str]

    @validator("tags", each_item=True)
    def validate_tag_item(cls, v):
        if len(v) < 3:
            raise ValueError("Элемент списка должен быть не короче 3 символов")
        return v


class SegmentActions(BaseModel):
    add_existed_tags: Optional[AddRemoveTags]
    remove_tags: Optional[AddRemoveTags]
    client_tags: Optional[CreateTags]
    send_tg_notification: Optional[TgNotificationsAction]
    add_docs_sales_tags: Optional[DocsSalesTags]
    remove_docs_sales_tags: Optional[DocsSalesTags]

