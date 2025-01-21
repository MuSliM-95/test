from pydantic import BaseModel


class OauthModel(BaseModel):
    client_id: str
    client_secret: str
