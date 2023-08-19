import aiohttp
import asyncio

async def main():
    async with aiohttp.ClientSession() as session1:
        async with session1.get(f'https://google.com') as resp:
            # amo_resp_json = await resp.json()
            print(resp)
            event_body = {
                "type": "amoevent",
                "name": "Обновление Access токена",
                "url": resp.url,
                # "payload": amo_post_json,
                "cashbox_id": "amoevent",
                "user_id": "amoevent",
                "token": "amoevent",
                "ip": "https://app.tablecrm.com",
                "promoimage": None,
                "promodata": None,
                "method": "POST",
            }
            print(event_body)
        await session1.close()

asyncio.run(main())