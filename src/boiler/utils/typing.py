from typing import Protocol


class AsyncBinaryReadable(Protocol):

    async def read(self, count: int, *args, **kwargs) -> bytes:
        raise NotImplementedError


class AsyncBinaryWritable(Protocol):

    async def write(self, data: bytes, *args, **kwargs) -> int:
        raise NotImplementedError
