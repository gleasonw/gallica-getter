from typing import AsyncGenerator, List
from pydantic import BaseModel
from gallicaGetter.fetch import fetch_queries_concurrently
from gallicaGetter.queries import ContentQuery
from gallicaGetter.utils.parse_xml import get_num_results_and_pages_for_context
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


class Context:
    """Wrapper for Gallica's ContentSearch API."""

    @staticmethod
    async def get(
        queries: List[ContentQuery],
        session: aiohttp.ClientSession,
    ) -> AsyncGenerator[HTMLContext, None]:
        for response in await fetch_queries_concurrently(
            queries=queries,
            session=session,
        ):
            if response:
                num_results_and_pages = get_num_results_and_pages_for_context(
                    response.text
                )
                yield HTMLContext(
                    num_results=num_results_and_pages[0],
                    pages=[
                        GallicaPage(page_label=occurrence[0], context=occurrence[1])
                        for occurrence in num_results_and_pages[1]
                    ],
                    ark=response.query.ark,
                )
