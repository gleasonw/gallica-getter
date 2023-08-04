from dataclasses import dataclass
from typing import AsyncGenerator, List
import aiohttp

from bs4 import BeautifulSoup
from gallicaGetter.fetch import fetch_queries_concurrently
from gallicaGetter.queries import FullTextQuery


@dataclass(slots=True)
class ParsedGallicaHTML:
    html: bytes
    soup: BeautifulSoup | None = None
    text: str = ""

    def __post_init__(self):
        self.soup = BeautifulSoup(markup=self.html, features="html.parser")

    @property
    def parsed_text(self) -> str:
        if self.soup:
            if hr_break_before_paras := self.soup.find("hr"):
                item_paras = hr_break_before_paras.find_next_siblings("p")
                self.text = "\n".join([para.text for para in item_paras])
        return self.text


class FullText:
    """Wraps Gallica's full text API. Can be an expensive fetch, and is rate-limited."""

    @staticmethod
    async def get(
        ark_codes, session: aiohttp.ClientSession | None = None
    ) -> AsyncGenerator[ParsedGallicaHTML, None]:
        if session is None:
            async with aiohttp.ClientSession() as session:
                async for result in FullText.get(ark_codes=ark_codes, session=session):
                    yield result

        if type(ark_codes) is not list:
            ark_codes = [ark_codes]
        queries = [FullTextQuery(ark=code) for code in ark_codes]
        for response in await fetch_queries_concurrently(
            queries=queries, session=session
        ):
            if response is not None:
                yield ParsedGallicaHTML(response.text)
