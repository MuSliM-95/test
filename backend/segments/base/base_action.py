

class BaseAction:
    def __init__(self, segment_obj):
        self.segment_obj = segment_obj
        self.ACTIONS = {}

    async def run(self, action, *args, **kwargs):
        if action in self.ACTIONS:
            await self.ACTIONS[action](*args, **kwargs)