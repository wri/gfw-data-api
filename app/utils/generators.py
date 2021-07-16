from typing import Any, AsyncGenerator, List


async def list_to_async_generator(input_list: List[Any]) -> AsyncGenerator[Any, None]:
    """Transform a List to an AsyncGenerator."""
    for i in input_list:
        yield i
