from api.nomenclature_groups.infrastructure.readers.core.INomenclatureGroupsReader import INomenclatureGroupsReader
from functions.helpers import get_user_by_token


class GetNomWithAttrFromGroupsView:

    def __init__(
        self,
        nomenclature_groups_reader: INomenclatureGroupsReader
    ):
        self.__nomenclature_groups_reader = nomenclature_groups_reader

    async def __call__(self, token: str, group_id: int):
        user = await get_user_by_token(token)

        group_info = await self.__nomenclature_groups_reader.get_nomen_with_attr(
            group_id=group_id,
            cashbox_id=user.cashbox_id,
        )

        return group_info