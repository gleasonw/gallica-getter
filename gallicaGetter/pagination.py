import asyncio
from typing import AsyncGenerator, Generator, List
import aiohttp
from gallicaGetter.fetch import fetch_queries_concurrently
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


class Pagination:
    """Wraps Gallica's Pagination API."""

    @staticmethod
    async def get(
        ark: str,
        session: aiohttp.ClientSession,
    ) -> PaginationData | None:
        query = PaginationQuery(ark=ark)
        response = list(
            await fetch_queries_concurrently(
                queries=[query],
                session=session,
            )
        )[0]
        if response is not None:
            elements = etree.fromstring(
                response.text, parser=etree.XMLParser(encoding="utf-8")
            )
            structure = elements.find("structure")
            if structure is not None:
                return PaginationData(
                    ark=response.query.ark,
                    page_count=len(elements.find("pages")),
                    has_toc=structure.find("hasToc").text == "true",
                    toc_location=structure.find("TocLocation").text,
                    has_content=structure.find("hasContent").text == "true",
                    nb_vue_images=structure.find("nbVueImages").text,
                    first_displayed_page=structure.find("firstDisplayedPage").text,
                )
