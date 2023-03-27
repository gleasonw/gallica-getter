import asyncio
from dataclasses import dataclass
import json
from typing import Callable, Generator, List, Tuple

import aiohttp
from pydantic import BaseModel
import pydantic
from gallicaGetter.context import HTMLContext
from gallicaGetter.fetch import Response, fetch_queries_concurrently
from gallicaGetter.gallicaWrapper import GallicaWrapper


@dataclass
class ContextSnippetQuery:
    ark: str
    term: str

    @property
    def params(self):
        return {}

    @property
    def endpoint_url(self):
        return f"https://gallica.bnf.fr/services/ajax/extract/ark:/12148/{self.ark}.r={self.term}"


class Snippet(BaseModel):
    contenu: str
    url: str


class Result(BaseModel):
    value: Snippet


class Fragment(BaseModel):
    contenu: List[Result]


class ExtractRoot(BaseModel):
    fragment: Fragment
    ark: str


class ContextSnippets(GallicaWrapper):
    def parse(self, gallica_responses):
        for response in gallica_responses:
            parsed_json = json.loads(response.text)
            try:
                yield ExtractRoot(**parsed_json, ark=response.query.ark)
            except pydantic.ValidationError:
                print("Error parsing response")
                print(parsed_json)

    async def get(
        self,
        context_pairs: List[Tuple[str, str]],
        on_receive_response: Callable[[Response], None] | None = None,
        session: aiohttp.ClientSession | None = None,
        semaphore: asyncio.Semaphore | None = None,
    ) -> Generator[ExtractRoot, None, None]:
        queries = [
            ContextSnippetQuery(ark=pair[0], term=pair[1]) for pair in context_pairs
        ]
        return self.parse(
            await fetch_queries_concurrently(
                queries=queries,
                session=session,
                semaphore=semaphore,
                on_receive_response=on_receive_response,
            )
        )