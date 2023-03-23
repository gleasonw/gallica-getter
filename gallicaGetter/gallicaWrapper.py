from dataclasses import dataclass
from typing import Any, Callable, Generator, Dict, List
import asyncio
import aiohttp
import time
import aiohttp.client_exceptions

from gallicaGetter.queries import (
    ContentQuery,
    FullTextQuery,
    IssuesQuery,
    PaperQuery,
    VolumeQuery,
)


@dataclass
class Response:
    xml: bytes
    query: VolumeQuery | PaperQuery | IssuesQuery | ContentQuery | FullTextQuery
    elapsed_time: float


class GallicaWrapper:
    """Base class for Gallica API wrappers."""

    def __init__(self):
        self.post_init()

    def get(self, **kwargs):
        raise NotImplementedError(
            f"get() not implemented for {self.__class__.__name__}"
        )

    def parse(
        self,
        gallica_responses: List[Response],
    ) -> Generator[Any, None, None]:
        raise NotImplementedError(
            f"parse() not implemented for {self.__class__.__name__}"
        )

    def post_init(self):
        pass

    async def get_records_for_queries(
        self,
        queries,
        session: aiohttp.ClientSession | None = None,
        semaphore: asyncio.Semaphore | None = None,
        on_receive_response: Callable[[Response], None] | None = None,
    ):
        """The core abstraction for fetching record xml from gallica and parsing it to Python objects. Called by all subclasses."""
        if session is None:
            async with aiohttp.ClientSession() as session:
                return await self.get_records_for_queries(
                    queries=queries,
                    session=session,
                    semaphore=semaphore,
                )
        raw_response = await fetch_from_gallica(
            queries,
            session=session,
            semaphore=semaphore,
            on_receive_response=on_receive_response,
        )

        return self.parse(raw_response)


async def fetch_from_gallica(
    queries,
    session: aiohttp.ClientSession,
    on_receive_response: Callable[[Response], None] | None = None,
    semaphore=None,
) -> List[Response]:
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
    query: VolumeQuery | PaperQuery | IssuesQuery | ContentQuery | FullTextQuery,
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore | None = None,
    on_receive_response: Callable[[Response], None] | None = None,
    num_retries=0,
) -> Response:
    if semaphore:
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
            xml=await response.content.read(),
            query=query,
            elapsed_time=elapsed_time,
        )
        if on_receive_response:
            on_receive_response(response)
        return response
