import asyncio


class CyclicQueue(asyncio.Queue):
    async def put(self, value):
        if self.maxsize is not None and self.qsize() == self.maxsize:
            await self.get()
        await super().put(value)

    def put_nowait(self, value):
        if self.maxsize is not None and self.qsize() == self.maxsize:
            super().get_nowait()
        super().put_nowait(value)
