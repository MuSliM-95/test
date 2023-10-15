from api.users import schemas as schemas
from fastapi import APIRouter
from functions import users as func

router = APIRouter(prefix="/users", tags=["users"])


@router.get("/", response_model=schemas.CBUsers)
async def get_user_by_token_route(token: str):
    return await func.get_user_by_token(token=token)
