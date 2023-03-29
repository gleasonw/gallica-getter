import asyncio
from dataclasses import dataclass
import time
from typing import Any, Callable, List
import aiohttp


@dataclass
class Response:
    text: bytes
    query: Any
    elapsed_time: float


async def fetch_queries_concurrently(
    queries,
    session: aiohttp.ClientSession | None = None,
    semaphore: asyncio.Semaphore | None = None,
    on_receive_response: Callable[[Response], None] | None = None,
) -> List[Response]:
    """The core abstraction for fetching data from Gallica concurrently. All requests pass through this function."""

    if session is None:
        async with aiohttp.ClientSession() as session:
            return await fetch_queries_concurrently(
                queries=queries,
                session=session,
                semaphore=semaphore,
            )

    if type(queries) is not list:
        queries = [queries]

    tasks = []
    for query in queries:
        tasks.append(
            get(
                query=query,
                session=session,
                on_receive_response=on_receive_response,
                semaphore=semaphore,
            )
        )

    return await asyncio.gather(*tasks)


# TODO: fix these typings, response for each query type
async def get(
    query,
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore | None = None,
    on_receive_response: Callable[[Response], None] | None = None,
    num_retries=0,
) -> Response:
    if num_retries > 3:
        raise aiohttp.ClientConnectionError("too many retries")
    if semaphore:
        print(semaphore._value)
        async with semaphore:
            return await get(
                query=query,
                session=session,
                on_receive_response=on_receive_response,
                semaphore=None,
            )
    start_time = time.perf_counter()
    async with session.get(query.endpoint_url, params=query.params) as response:
        elapsed_time = time.perf_counter() - start_time
        # check if we need to retry
        if response.status != 200 and num_retries < 3:
            print(f"retrying {num_retries}")
            print(response.status)
            await asyncio.sleep(2**num_retries)
            return await get(
                query=query,
                session=session,
                on_receive_response=on_receive_response,
                semaphore=semaphore,
                num_retries=num_retries + 1,
            )
        response = Response(
            text=await response.content.read(),
            query=query,
            elapsed_time=elapsed_time,
        )
        if on_receive_response:
            on_receive_response(response)
        return response
