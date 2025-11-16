from dataclasses import dataclass
import json
from typing import AsyncGenerator, List, Optional
import urllib.parse

import aiohttp
from pydantic import BaseModel
import pydantic
from app.fetch import fetch_queries_concurrently


@dataclass
class ContextSnippetQuery:
    """
    Uses the Gallica ajax service, which has more robust support for link_terms and link_distance than the ContentSearch API.
    Formerly, it was also helpful in reducing the amount of context returned for a query. Now, it seems like Gallica has added a 10 record limit on ContentSearch API responses,
    so the "sample context" functionality doesn't really apply anymore. This api is just helpful for link_term and link_distance.
    """
    ark: str
    term: str
    link_term: Optional[str] = None
    link_distance: Optional[int] = None

    @property
    def params(self):
        return {}

    @property
    def endpoint_url(self):
        base = f"https://gallica.bnf.fr/services/ajax/extract/ark:/12148/{self.ark}.r="
        # Build r= parameter; if link_term and distance provided, use prox expression
        if self.link_term and self.link_distance is not None:
            # Format: (prOx: "term" distance "link_term") — keep parentheses unescaped as in observed URLs
            prox_expr = f"(prOx: \"{self.term}\" {self.link_distance} \"{self.link_term}\")"
            encoded = urllib.parse.quote(prox_expr, safe="()")
            return base + encoded
        # Fallback to simple single-term search
        encoded_term = urllib.parse.quote(self.term, safe="")
        return base + encoded_term


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
            if response is not None:
                parsed_json = json.loads(response.text)
                try:
                    yield ExtractRoot(**parsed_json, ark=response.query.ark)
                except pydantic.ValidationError:
                    print("Error parsing response")
                    print(parsed_json)
