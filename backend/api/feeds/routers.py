import hashlib
import os
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException
from starlette.responses import Response

from . import schemas
from functions.helpers import get_user_by_token

from database.db import feeds, database

from .feed_generator.generator import FeedGenerator

router = APIRouter(tags=["feeds"])


def generate_feed_token() -> str:
    # md5 от uuid4 + random salt
    raw = (str(uuid.uuid4()) + os.urandom(8).hex()).encode("utf-8")
    return hashlib.md5(raw).hexdigest()


@router.post("/feeds")
async def create_feed(
    token: str,
    data: schemas.FeedCreate
):
    user = await get_user_by_token(token)

    url_token = generate_feed_token()

    data = data.dict()
    data["cashbox_id"] = user.cashbox_id
    data["url_token"] = url_token

    query = feeds.insert().values(data).returning(feeds.c.id)
    feed_id = await database.execute(query)
    return schemas.Feed(
        id=feed_id,
        **data,
    )


@router.get("/feeds/{url_token}")
async def get_feed(
        url_token: str,
):
    query = feeds.select().where(feeds.c.url_token == url_token)
    feed = await database.fetch_one(query)
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")

    generator = FeedGenerator(url_token)
    result = await generator.generate()
    return result if result is not None else None


@router.get("/feeds")
async def get_feeds(token: str):
    user = await get_user_by_token(token)

    query = feeds.select().where(feeds.c.cashbox_id == user.cashbox_id)

    db_feeds = await database.fetch_all(query)

    return schemas.GetFeeds(
        count=len(db_feeds),
        feeds=db_feeds
    )


@router.patch("/feeds/{idx}")
async def update_feed(
        token: str,
        idx: int,
        data: schemas.FeedUpdate
):
    user = await get_user_by_token(token)
    query = feeds.select().where(
        feeds.c.id == idx,
        feeds.c.cashbox_id == user.cashbox_id
    )
    feed = await database.fetch_one(query)
    if feed is None:
        raise HTTPException(status_code=404, detail="Feed not found")
    resp = dict(feed)

    upd_data = data.dict(exclude_none=True)
    update_query = feeds.update().where(feeds.c.id == idx).values(**upd_data)
    await database.execute(update_query)

    for k, v in upd_data.items():
        resp[k] = v

    return schemas.Feed(**resp)


@router.delete("/feeds/{idx}")
async def delete_feed(
        token: str,
        idx: int
):
    user = await get_user_by_token(token)
    query = feeds.delete().where(
        feeds.c.id == idx,
        feeds.c.cashbox_id == user.cashbox_id
    )
    await database.execute(query)
    return Response(status_code=204)




