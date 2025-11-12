from fastapi import HTTPException
from sqlalchemy import select
from database.db import database, users_cboxes_relation


async def get_current_user(token: str):
    if not token:
        raise HTTPException(status_code=401, detail="Token required")
    
    query = select([
        users_cboxes_relation.c.id,
        users_cboxes_relation.c.user,
        users_cboxes_relation.c.cashbox_id,
        users_cboxes_relation.c.token,
        users_cboxes_relation.c.status,
        users_cboxes_relation.c.is_owner
    ]).where(
        users_cboxes_relation.c.token == token
    )
    user = await database.fetch_one(query)
    
    if not user:
         raise HTTPException(status_code=401, detail="Invalid token")
    
    if not user.status:
        raise HTTPException(status_code=403, detail="User inactive")
    
    return user


async def get_current_user_owner(token: str):
    user = await get_current_user(token)
    
    if not user.is_owner:
        raise HTTPException(status_code=403, detail="Owner permissions required")
    
    return user
