from typing import List, Optional

from pydantic import BaseModel

from api.nomenclature_groups.infrastructure.models.NomenclatureGroupModel import NomenclatureGroupModel

class Nomenclature(BaseModel):
    id: int
    name: Optional[str]
    is_main: bool

class GroupModelWithNomenclaturesModel(NomenclatureGroupModel):
    nomenclatures: Optional[List[Nomenclature]]