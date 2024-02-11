from typing import AsyncGenerator, List

import aiohttp
from app.fetch import fetch_queries_concurrently
from app.queries import IssuesQuery
from app.utils.parse_xml import get_years_published
from dataclasses import dataclass


@dataclass
class IssueYearRecord:
    code: str
    years: List[str]


class Issues:
    """Fetches periodical periodical publishing years from Gallica's Issues API. Used in PapersWrapper."""

    @staticmethod
    async def get(
        codes, session: aiohttp.ClientSession
    ) -> AsyncGenerator[IssueYearRecord, None]:
        if type(codes) == str:
            codes = [codes]
        queries = [IssuesQuery(code=code) for code in codes]
        for response in await fetch_queries_concurrently(queries, session=session):
            if response is not None:
                years = get_years_published(response.text)
                code = response.query.code
                yield IssueYearRecord(code=code, years=years)
