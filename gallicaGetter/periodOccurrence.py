from dataclasses import dataclass

import aiohttp
from gallicaGetter.fetch import fetch_queries_concurrently
from gallicaGetter.utils.date import Date
from gallicaGetter.utils.parse_xml import get_num_records_from_gallica_xml
from gallicaGetter.gallicaWrapper import GallicaWrapper
from gallicaGetter.utils.base_query_builds import build_base_queries
from typing import Generator, List, Literal, Optional

from models import OccurrenceArgs


@dataclass(slots=True)
class PeriodRecord:
    _date: Date
    count: float
    term: str

    @property
    def year(self):
        return self._date.year

    @property
    def month(self):
        return self._date.month

    @property
    def day(self):
        return self._date.day


class PeriodOccurrence(GallicaWrapper):
    """Fetches # occurrences of terms in a given period of time. Useful for making graphs."""

    async def get(
        self,
        args: OccurrenceArgs,
        grouping: Literal["year", "month"] = "year",
        onProgressUpdate=None,
        session: aiohttp.ClientSession | None = None,
    ) -> Generator[PeriodRecord, None, None]:
        queries = build_base_queries(
            args=args,
            grouping=grouping,
        )
        return self.parse(
            await fetch_queries_concurrently(
                queries=queries, on_receive_response=onProgressUpdate, session=session
            )
        )

    def parse(self, gallica_responses):
        for response in gallica_responses:
            count = get_num_records_from_gallica_xml(response.text)
            query = response.query
            yield PeriodRecord(
                _date=Date(query.start_date),
                count=count,
                term=query.terms,
            )
