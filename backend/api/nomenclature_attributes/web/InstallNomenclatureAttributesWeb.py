from fastapi import FastAPI
from starlette import status

from api.nomenclature_attributes.web.models.schemas import AttributeCreateResponse, AttributeValueResponse, \
    NomenclatureWithAttributesResponse
from api.nomenclature_attributes.web.views.AddNomenclatureAttributeValueView import AddNomenclatureAttributeValueView
from api.nomenclature_attributes.web.views.CreateNomenclatureAttributesView import CreateNomenclatureAttributesView
from api.nomenclature_attributes.web.views.GetNomenclatureAttributesView import GetNomenclatureAttributesView


class InstallNomenclatureAttributesWeb:

    def __call__(self, app: FastAPI):
        create_nomenclature_attributes_view = CreateNomenclatureAttributesView()

        add_nomenclature_attribute_value_view = AddNomenclatureAttributeValueView()

        get_nomenclature_attributes_view = GetNomenclatureAttributesView()

        app.add_api_route(
            path="/nomenclature/attribute",
            endpoint=create_nomenclature_attributes_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            response_model=AttributeCreateResponse,
            tags=["nomenclature_attributes"]
        )

        app.add_api_route(
            path="/nomenclature/attributes_value",
            endpoint=add_nomenclature_attribute_value_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            response_model=AttributeValueResponse,
            tags=["nomenclature_attributes"]
        )

        app.add_api_route(
            path="/nomenclature/{nomenclature_id}/attributes",
            endpoint=get_nomenclature_attributes_view.__call__,
            methods=["GET"],
            status_code=status.HTTP_200_OK,
            response_model=NomenclatureWithAttributesResponse,
            tags=["nomenclature_attributes"]
        )