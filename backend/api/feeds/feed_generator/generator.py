import json
from xml.sax.saxutils import escape

from fastapi import HTTPException

from database.db import feeds, database

from api.feeds.feed_generator.criterias.filters import FeedCriteriaFilter


from api.feeds.schemas import ALLOWED_DB_FIELDS
from starlette.responses import Response


class FeedGenerator:

    def __init__(self, url_token: str) -> None:
        self.url_token = url_token
        self.feed = None

    async def get_feed(self):
        if self.url_token:
            query = feeds.select().where(feeds.c.url_token == self.url_token)
            self.feed = await database.fetch_one(query)

        return self.feed

    async def generate(self):
        feed = await self.get_feed()
        if not feed:
            return None

        filter = FeedCriteriaFilter(json.loads(self.feed.criteria), self.feed.cashbox_id)
        balance = await filter.get_warehouse_balance()

        root_tag = feed["root_tag"]
        item_tag = feed["item_tag"]
        tags_map = feed["field_tags"]

        parts = [f'<?xml version="1.0" encoding="utf-8"?>\n<{root_tag}>\n']
        for r in balance:
            parts.append(f"  <{item_tag}>\n")
            for xml_tag, field in tags_map.items():
                val = r.get(field)
                if val is None:
                    continue
                elif isinstance(val, list):
                    for v in val:
                        text = escape(str(v))
                        parts.append(f"    <{xml_tag}>{text}</{xml_tag}>\n")

                elif isinstance(val, dict):
                    if field == "params":
                        for k, v in val.items():
                            parts.append(f'    <{xml_tag} name="{k}">{v}</{xml_tag}>\n')

                else:
                    text = escape(str(val))
                    parts.append(f"    <{xml_tag}>{text}</{xml_tag}>\n")
            parts.append(f"  </{item_tag}>\n")
        parts.append(f"</{root_tag}>")
        xml_str = "".join(parts).encode("utf-8")

        response = Response(content=xml_str, media_type="application/xml")
        response.headers["Cache-Control"] = "public, max-age=60"
        return response




