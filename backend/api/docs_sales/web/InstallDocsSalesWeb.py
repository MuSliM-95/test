from fastapi import FastAPI
from starlette import status

from api.docs_sales.web.views.CreateDocsSalesView import CreateDocsSalesView


class InstallDocsSalesWeb:

    def __call__(
        self,
        app: FastAPI
    ):
        create_docs_sales_view = CreateDocsSalesView()

        app.add_api_route(
            path="/docs_sales/test",
            endpoint=create_docs_sales_view.__call__,
            methods=["POST"],
            status_code=status.HTTP_200_OK,
            tags=["docs_sales"]
        )