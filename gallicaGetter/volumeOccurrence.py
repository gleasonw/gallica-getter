import asyncio
from dataclasses import dataclass
import urllib.parse

import aiohttp
from gallicaGetter.fetch import fetch_queries_concurrently
from gallicaGetter.queries import VolumeQuery

from gallicaGetter.utils.base_query_builds import build_base_queries
from gallicaGetter.utils.index_query_builds import (
    build_indexed_queries,
    index_queries_by_num_results,
)
from gallicaGetter.utils.date import Date
from gallicaGetter.gallicaWrapper import GallicaWrapper, Response
from gallicaGetter.utils.parse_xml import (
    get_author_from_record_xml,
    get_ocr_quality_from_record_xml,
    get_records_from_xml,
    get_paper_title_from_record_xml,
    get_paper_code_from_record_xml,
    get_date_from_record_xml,
    get_url_from_record,
    get_num_records_from_gallica_xml,
)

from typing import Callable, Generator, List, Literal, Optional, Tuple


@dataclass(frozen=True, slots=True)
class VolumeRecord:
    paper_title: str
    paper_code: str
    ocr_quality: float
    author: str
    url: str
    date: Date
    terms: List[str]

    @property
    def ark(self) -> str:
        return self.url.split("/")[-1]

    def dict(self):
        return {
            "paper_title": self.paper_title,
            "paper_code": self.paper_code,
            "ocr_quality": self.ocr_quality,
            "author": self.author,
            "url": self.url,
            "date": str(self.date),
            "terms": self.terms,
            "ark": self.ark,
        }


class VolumeOccurrence(GallicaWrapper):
    """Fetches occurrence metadata from Gallica's SRU API. There may be many occurrences in one Gallica record."""

    def parse(
        self,
        gallica_responses: List[Response],
    ):
        for response in gallica_responses:
            for i, record in enumerate(get_records_from_xml(response.text)):
                if self.on_get_total_records and i == 0:
                    self.on_get_total_records(
                        get_num_records_from_gallica_xml(response.text)
                    )
                assert isinstance(response.query, VolumeQuery)
                yield VolumeRecord(
                    paper_title=get_paper_title_from_record_xml(record),
                    paper_code=get_paper_code_from_record_xml(record),
                    date=get_date_from_record_xml(record),
                    url=get_url_from_record(record),
                    author=get_author_from_record_xml(record),
                    ocr_quality=float(get_ocr_quality_from_record_xml(record)),
                    terms=response.query.terms,
                )

    def post_init(self):
        self.on_get_total_records: Optional[Callable[[int], None]] = None

    async def get(
        self,
        terms: List[str],
        source: Optional[Literal["book", "periodical", "all"]] = "all",
        link: Optional[Tuple[str, int]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        codes: Optional[List[str]] = None,
        limit: Optional[int] = None,
        start_index: int | List[int] = 0,
        sort: Optional[Literal["date", "relevance"]] = None,
        query_cache=None,
        on_get_total_records: Optional[Callable[[int], None]] = None,
        on_get_origin_urls: Optional[Callable[[List[str]], None]] = None,
        get_all_results: bool = False,
        on_receive_response: Optional[Callable[[Response], None]] = None,
        session: aiohttp.ClientSession | None = None,
        semaphore: asyncio.Semaphore | None = None,
    ) -> Generator[VolumeRecord, None, None]:
        if session is None:
            async with aiohttp.ClientSession() as session:
                local_args = locals()
                del local_args["self"]
                del local_args["session"]
                return await self.get(**local_args, session=session)
        if query_cache:
            queries = index_queries_by_num_results(query_cache)
        else:
            base_queries = build_base_queries(
                terms=terms,
                start_date=start_date,
                end_date=end_date,
                codes=codes,
                link=link,
                source=source,
                sort=sort,
                grouping="all",
                limit=limit,
                cursor=start_index,
            )
            if (limit and limit > 50) or get_all_results:
                # assume we want all results, or index for more than 50
                # we will have to fetch # total records from Gallica
                queries = await build_indexed_queries(
                    base_queries,
                    session=session,
                    limit=limit,
                )
            else:
                # num results less than 50, the base query is fine
                queries = base_queries
        if on_get_total_records:
            # If we want to know the total number of records, we need to assign the callback
            self.on_get_total_records = on_get_total_records
        if on_get_origin_urls:
            url = "https://gallica.bnf.fr/SRU?"
            on_get_origin_urls(
                [url + urllib.parse.urlencode(query.params) for query in queries]
            )
        return self.parse(
            await fetch_queries_concurrently(
                queries=queries,
                on_receive_response=on_receive_response,
                session=session,
                semaphore=semaphore,
            )
        )
