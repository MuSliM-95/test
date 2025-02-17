import base64
import os

from apps.yookassa.services.core.IOauthService import IOauthService

from fastapi import  Request, Depends
from fastapi.responses import Response


class EventWebhookView:

    async def __call__(self, request: Request):

        print(await request.json())

        return Response(status_code=200)
