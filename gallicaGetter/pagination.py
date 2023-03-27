import asyncio
from typing import Generator, List
import aiohttp
from gallicaGetter.fetch import fetch_queries_concurrently
from gallicaGetter.gallicaWrapper import GallicaWrapper, Response
from pydantic import BaseModel
from lxml import etree


class PaginationQuery(BaseModel):
    ark: str

    @property
    def endpoint_url(self):
        return "https://gallica.bnf.fr/services/Pagination"

    @property
    def params(self):
        return {
            "ark": self.ark,
        }


class PaginationData(BaseModel):
    ark: str
    page_count: int
    has_toc: bool
    toc_location: int
    has_content: bool
    nb_vue_images: int
    first_displayed_page: int


class Pagination(GallicaWrapper):
    """Wraps Gallica's Pagination API."""

    def parse(self, gallica_responses: List[Response]):
        if not gallica_responses or len(gallica_responses) != 1:
            return []
        response = gallica_responses[0]
        try:
            elements = etree.fromstring(
                response.text, parser=etree.XMLParser(encoding="utf-8")
            )
        except etree.XMLSyntaxError:
            return []
        structure = elements.find("structure")
        if structure is not None:
            yield PaginationData(
                ark=response.query.ark,
                page_count=len(elements.find("pages")),
                has_toc=structure.find("hasToc").text == "true",
                toc_location=structure.find("TocLocation").text,
                has_content=structure.find("hasContent").text == "true",
                nb_vue_images=structure.find("nbVueImages").text,
                first_displayed_page=structure.find("firstDisplayedPage").text,
            )

    async def get(
        self,
        ark: str,
        session: aiohttp.ClientSession | None = None,
        semaphore: asyncio.Semaphore | None = None,
    ) -> Generator[PaginationData, None, None]:
        query = PaginationQuery(ark=ark)
        return self.parse(
            await fetch_queries_concurrently(
                queries=[query],
                session=session,
                semaphore=semaphore,
            )
        )
