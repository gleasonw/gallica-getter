import asyncio
from typing import Callable, Generator, List, Tuple
from pydantic import BaseModel
from gallicaGetter.fetch import fetch_queries_concurrently
from gallicaGetter.queries import ContentQuery
from gallicaGetter.utils.parse_xml import get_num_results_and_pages_for_context
from gallicaGetter.gallicaWrapper import GallicaWrapper, Response
from dataclasses import dataclass
import aiohttp


class GallicaPage(BaseModel):
    page_label: str
    context: str

    @property
    def page_num(self):
        if page_num := self.page_label.split("_")[-1]:
            if page_num.isdigit():
                return int(page_num)


class HTMLContext(BaseModel):
    num_results: int
    pages: List[GallicaPage]
    ark: str


class Context(GallicaWrapper):
    """Wrapper for Gallica's ContentSearch API."""

    def parse(self, gallica_responses):
        for response in gallica_responses:
            num_results_and_pages = get_num_results_and_pages_for_context(response.text)
            yield HTMLContext(
                num_results=num_results_and_pages[0],
                pages=[
                    GallicaPage(page_label=occurrence[0], context=occurrence[1])
                    for occurrence in num_results_and_pages[1]
                ],
                ark=response.query.ark,
            )

    async def get(
        self,
        context_pairs: List[Tuple[str, List[str]]],
        session: aiohttp.ClientSession,
    ) -> Generator[HTMLContext, None, None]:
        queries: List[ContentQuery] = []
        for pair in context_pairs:
            terms = pair[1]
            wrapped_terms: List[str] = []
            # TODO: wrap terms in quotes for exact search in document... might be a better way
            for term in terms:
                if term.startswith('"') and term.endswith('"'):
                    wrapped_terms.append(term)
                else:
                    if " " in term:
                        wrapped_terms.append(f'"{term}"')
                    else:
                        wrapped_terms.append(term)
            queries.append(ContentQuery(ark=pair[0], terms=wrapped_terms))
        return self.parse(
            await fetch_queries_concurrently(
                queries=queries,
                session=session,
            )
        )
