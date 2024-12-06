from typing import Dict, List
from jobs.autoburn_job.job import AutoBurn


class TriggersNotification(AutoBurn):
    async def test(self) -> None:
        return await self.transactions()


async def run():
    triggers = await TriggersNotification.get_cards()
    for trigger in triggers:
        print(await TriggersNotification(card = trigger).test())



