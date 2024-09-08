class IPostLeadEvent:

    async def __call__(self):
        raise NotImplementedError()