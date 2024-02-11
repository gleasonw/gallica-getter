from dataclasses import dataclass
from typing import List, Optional

import aiohttp
from app.fetch import fetch_queries_concurrently
from app.issues import Issues
from app.utils.parse_xml import (
    get_paper_code_from_record_xml,
    get_paper_title_from_record_xml,
    get_records_from_xml,
    get_url_from_record,
)
from app.queries import PaperQuery
from app.utils.base_query_builds import (
    bundle_codes,
    NUM_CODES_PER_BUNDLE,
)
from app.utils.index_query_builds import build_indexed_queries


@dataclass(slots=True)
class PaperRecord:
    code: str
    title: str
    url: str
    publishing_years: List[str]

    @property
    def continuous(self):
        if self.publishing_years:
            return int(self.publishing_years[-1]) - int(
                self.publishing_years[0]
            ) + 1 == len(self.publishing_years)
        else:
            return False


class Papers:
    """There is no official Gallica endpoint for fetching paper metadata. This class fetches from two Gallica endpoints, SRU (titles, codes) and Issues (publishing years), to get all metadata."""

    @staticmethod
    async def get(
        arg_codes: Optional[List[str]] = None,
        get_all_results: bool = False,
        session: aiohttp.ClientSession | None = None,
    ) -> List[PaperRecord]:
        if session is None:
            async with aiohttp.ClientSession() as session:
                return await Papers.get(arg_codes, get_all_results, session)
        if not arg_codes and get_all_results:
            # Fetch all results, indexing by the number of papers on Gallica. Lengthy fetch.
            queries = build_indexed_queries(
                [PaperQuery(start_index=0, limit=1)],
                session=session,
            )
        elif not arg_codes and not get_all_results:
            raise ValueError(
                "Must provide arg_codes to get specific papers, or set get_all_results=True"
            )
        else:
            queries = [
                PaperQuery(
                    start_index=0,
                    limit=NUM_CODES_PER_BUNDLE,
                    codes=code_bundle,
                )
                for code_bundle in bundle_codes(arg_codes)
            ]
        sru_paper_records = [
            PaperRecord(
                code=get_paper_code_from_record_xml(record),
                title=get_paper_title_from_record_xml(record),
                url=get_url_from_record(record),
                publishing_years=[],
            )
            for response in await fetch_queries_concurrently(queries, session=session)
            for record in get_records_from_xml(response.text)
        ]
        paper_codes = [record.code for record in sru_paper_records]
        year_records = Issues.get(paper_codes, session=session)
        years_as_dict = {record.code: record.years async for record in year_records}
        for record in sru_paper_records:
            record.publishing_years = years_as_dict[record.code]
        return sru_paper_records
