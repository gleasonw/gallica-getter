from dataclasses import dataclass
import urllib.parse

import aiohttp
from app.fetch import fetch_queries_concurrently
from app.queries import VolumeQuery

from app.utils.base_query_builds import build_base_queries
from app.utils.index_query_builds import (
    build_indexed_queries,
    index_queries_by_num_results,
)
from app.utils.parse_xml import (
    get_author_from_record_xml,
    get_ocr_quality_from_record_xml,
    get_records_from_xml,
    get_paper_title_from_record_xml,
    get_paper_code_from_record_xml,
    get_date_from_record_xml,
    get_url_from_record,
    get_num_records_from_gallica_xml,
    get_publisher_from_record_xml,
)

from typing import Any, Callable, Generator, List, Optional

from app.models import OccurrenceArgs


@dataclass(frozen=True, slots=True)
class VolumeRecord:
    paper_title: str
    paper_code: str
    ocr_quality: float
    author: str
    url: str
    date: str
    terms: List[str]
    publisher: Optional[str] = None

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


class VolumeOccurrence:
    """Fetches occurrence metadata from Gallica's SRU API. There may be many occurrences in one Gallica record."""

    @staticmethod
    async def get_custom_query(
        query: VolumeQuery,
        session: aiohttp.ClientSession,
        on_get_origin_urls: Optional[Callable[[List[str]], None]] = None,
    ):
        base_queries = index_queries_by_num_results([query])
        if on_get_origin_urls:
            url = "https://gallica.bnf.fr/SRU?"
            on_get_origin_urls(
                [url + urllib.parse.urlencode(query.params) for query in base_queries]
            )
        return VolumeOccurrence.parse(
            gallica_responses=await fetch_queries_concurrently(
                queries=base_queries,
                session=session,
            )
        )

    @staticmethod
    async def get(
        args: OccurrenceArgs,
        on_get_total_records: Optional[Callable[[int], None]] = None,
        on_get_origin_urls: Optional[Callable[[List[str]], None]] = None,
        get_all_results: bool = False,
        session: aiohttp.ClientSession | None = None,
    ) -> Generator[VolumeRecord, None, None]:
        if session is None:
            async with aiohttp.ClientSession() as session:
                local_args = locals()
                del local_args["session"]
                return await VolumeOccurrence.get(**local_args, session=session)
        else:
            base_queries = build_base_queries(
                args=args,
                grouping="all",
            )
            if (args.limit and args.limit > 50) or get_all_results:
                # assume we want all results, or index for more than 50
                # we will have to fetch # total records from Gallica
                queries = await build_indexed_queries(
                    base_queries,
                    session=session,
                    limit=args.limit,
                    on_get_total_records=on_get_total_records,
                )
            else:
                # num results less than 50, the base query is fine
                queries = base_queries
        if on_get_origin_urls:
            url = "https://gallica.bnf.fr/SRU?"
            on_get_origin_urls(
                [url + urllib.parse.urlencode(query.params) for query in queries]
            )
        return VolumeOccurrence.parse(
            gallica_responses=await fetch_queries_concurrently(
                queries=queries, session=session
            ),
            on_get_total_records=on_get_total_records,
        )

    @staticmethod
    def parse(gallica_responses: Any, on_get_total_records=None):
        for response in gallica_responses:
            if response is not None:
                for i, record in enumerate(get_records_from_xml(response.text)):
                    if i == 0 and on_get_total_records:
                        on_get_total_records(
                            get_num_records_from_gallica_xml(response.text)
                        )
                    assert isinstance(response.query, VolumeQuery)
                    yield VolumeRecord(
                        paper_title=get_paper_title_from_record_xml(record),
                        paper_code=get_paper_code_from_record_xml(record),
                        date=get_date_from_record_xml(record),
                        url=get_url_from_record(record),
                        author=get_author_from_record_xml(record),
                        publisher=get_publisher_from_record_xml(record),
                        ocr_quality=float(get_ocr_quality_from_record_xml(record)),
                        terms=response.query.terms,
                    )
