from pydantic import BaseModel
from typing import Optional, List, Union

# Создание атрибутов для номенклатур
class AttributeCreate(BaseModel):
    name: str
    alias: Optional[str] = None

class AttributeCreateResponse(BaseModel):
    id: int
    name: str
    alias: Optional[str] = None

#Создание значений для атрибутов номенклатур
class AttributeValue(BaseModel):
    attribute_id: int
    value: Union[str, List[str]]

class AttributeValueCreate(BaseModel):
    nomenclature_id: int
    attributes: List[AttributeValue]

class AttributeValueResponse(BaseModel):
    nomenclature_id: int
    attributes: List[AttributeValue]

# Вывод
class AttributeResponse(BaseModel):
    name: str
    value: Union[str, List[str]]

class NomenclatureWithAttributesResponse(BaseModel):
    id: int
    name: str
    attributes: List[AttributeResponse]

class NomenclatureAttribute(BaseModel):
    name: str
    value: str

class NomenclatureRelations(BaseModel):
    nomenclature_ids: List[int]

    class Config:
        schema_extra = {
            "example": {
                "nomenclature_ids": []
            }
        }

class AddNomenclatureRequest(BaseModel):
    group_id: int
    nomenclature_id: int

class AddNomenclatureResponse(BaseModel):
    message: str
    group_id: int
    nomenclature_id: int

class NomenclatureGroupResponse(BaseModel):
    nomenclature_id: int
    group_id: int