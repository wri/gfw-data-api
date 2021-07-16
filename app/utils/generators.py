from typing import Any, AsyncGenerator, List


async def list_to_async_generator(input: List[Any]) -> AsyncGenerator[Any, None]:
    """Yield numbers from 0 to `to` every `delay` seconds."""
    for i in input:
        yield i
