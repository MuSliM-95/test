from apps.yookassa.services.core.IYookassaApiService import IYookassaApiService


class CreatePaymentApiView:

    def __init__(
            self,
            yookassa_api_service: IYookassaApiService
    ):
        self.__yookassa_api_service = yookassa_api_service


    def __call__(self):
        pass