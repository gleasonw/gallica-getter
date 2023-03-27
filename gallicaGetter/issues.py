from typing import Generator, List

import aiohttp
from gallicaGetter.fetch import fetch_queries_concurrently
from gallicaGetter.queries import IssuesQuery
from gallicaGetter.utils.parse_xml import get_years_published
from gallicaGetter.gallicaWrapper import GallicaWrapper
from dataclasses import dataclass


@dataclass
class IssueYearRecord:
    code: str
    years: List[str]


class Issues(GallicaWrapper):
    """Fetches periodical periodical publishing years from Gallica's Issues API. Used in PapersWrapper."""

    def parse(self, gallica_responses):
        for response in gallica_responses:
            years = get_years_published(response.text)
            code = response.query.code
            yield IssueYearRecord(code=code, years=years)

    async def get(
        self, codes, session: aiohttp.ClientSession | None = None
    ) -> Generator[IssueYearRecord, None, None]:
        if type(codes) == str:
            codes = [codes]
        queries = [IssuesQuery(code=code) for code in codes]
        return self.parse(await fetch_queries_concurrently(queries, session=session))
