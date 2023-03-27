from dataclasses import dataclass
from typing import Generator, List

from bs4 import BeautifulSoup
from gallicaGetter.fetch import fetch_queries_concurrently
from gallicaGetter.queries import FullTextQuery
from gallicaGetter.gallicaWrapper import GallicaWrapper


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


class FullText(GallicaWrapper):
    """Wraps Gallica's full text API. Can be an expensive fetch, and is rate-limited."""

    def parse(self, gallica_responses):
        return (ParsedGallicaHTML(response.text) for response in gallica_responses)

    async def get(self, ark_codes) -> Generator[ParsedGallicaHTML, None, None]:
        if type(ark_codes) is not list:
            ark_codes = [ark_codes]
        queries = [FullTextQuery(ark=code) for code in ark_codes]
        return self.parse(await fetch_queries_concurrently(queries=queries))
