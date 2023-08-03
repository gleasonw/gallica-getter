from dataclasses import dataclass
import json
from typing import AsyncGenerator, List

import aiohttp
from pydantic import BaseModel
import pydantic
from gallicaGetter.fetch import fetch_queries_concurrently


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

    @property
    def page_num(self):
        f_item = self.url.split("/")[-1].split(".")[0]
        if f_item[1:].isdigit():
            return int(f_item[1:])


class Result(BaseModel):
    value: Snippet

    @property
    def context(self):
        """Small abstraction to obscure the details of the JSON structure from downstream."""
        return self.value.contenu

    @property
    def page_num(self):
        return self.value.page_num


class Fragment(BaseModel):
    contenu: List[Result]


class ExtractRoot(BaseModel):
    fragment: Fragment
    ark: str

    @property
    def pages(self):
        """Small abstraction to obscure the details of the JSON structure from downstream."""
        return self.fragment.contenu


class ContextSnippets:
    @staticmethod
    async def get(
        queries: List[ContextSnippetQuery],
        session: aiohttp.ClientSession | None = None,
    ) -> AsyncGenerator[ExtractRoot, None]:
        if session is None:
            async with aiohttp.ClientSession() as session:
                async for result in ContextSnippets.get(queries, session):
                    yield result

        for response in await fetch_queries_concurrently(
            queries=queries,
            session=session,
        ):
            parsed_json = json.loads(response.text)
            try:
                yield ExtractRoot(**parsed_json, ark=response.query.ark)
            except pydantic.ValidationError:
                print("Error parsing response")
                print(parsed_json)
