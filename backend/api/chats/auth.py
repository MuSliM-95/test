from fastapi import HTTPException, Header, Query
from typing import Optional
from sqlalchemy import select
from database.db import database, users_cboxes_relation


async def get_current_user(
    token: Optional[str] = Query(None, description="User authentication token"),
    authorization: Optional[str] = Header(None, description="Authorization header (Bearer token or token)")
):
    if not token:
        if authorization:
            token = authorization.replace("Bearer ", "").strip()
    
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


async def get_current_user_owner(
    token: Optional[str] = Query(None, description="User authentication token"),
    authorization: Optional[str] = Header(None, description="Authorization header (Bearer token or token)")
):
    user = await get_current_user(token=token, authorization=authorization)
    
    if not user.is_owner:
        raise HTTPException(status_code=403, detail="Owner permissions required")
    
    return user
