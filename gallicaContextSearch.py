import asyncio
import aiohttp.client_exceptions
from gallicaGetter.pageText import ConvertedXMLPage, PageQuery, PageText
from gallicaGetter.volumeOccurrence import VolumeOccurrence, VolumeRecord
from gallicaGetter.context import Context, HTMLContext
from typing import Callable, Dict, List, Literal, Optional, Tuple
from pydantic import BaseModel
from functools import partial
from bs4 import BeautifulSoup
import aiohttp
from contextSearchArgs import ContextSearchArgs

MAX_CONCURRENT_REQUESTS_FOR_CSV = 20


class ContextRow(BaseModel):
    pivot: str
    left_context: str
    right_context: str
    page_url: str
    page: str


class GallicaRecordBase(BaseModel):
    paper_title: str
    paper_code: str
    ark: str
    terms: List[str]
    date: str
    url: str
    author: str
    ocr_quality: float


class GallicaRecordWithRows(GallicaRecordBase):
    context: List[ContextRow]


class GallicaRecordWithHTML(GallicaRecordBase):
    context: HTMLContext


class GallicaRecordWithPages(GallicaRecordBase):
    context: List[Dict[str, str | int]]


def build_html_record(record: VolumeRecord, context: HTMLContext):
    return GallicaRecordWithHTML(
        paper_title=record.paper_title,
        paper_code=record.paper_code,
        terms=record.terms,
        date=str(record.date),
        url=record.url,
        context=context,
        ark=record.ark,
        author=record.author,
        ocr_quality=record.ocr_quality,
    )


def build_row_record(record: VolumeRecord, context: HTMLContext):
    """Split the Gallica HTML context on the highlighted spans, creating rows of pivot (span), left context, and right context."""
    rows: List[ContextRow] = []

    for page in context.pages:
        soup = BeautifulSoup(page.context, "html.parser")
        spans = soup.find_all("span", {"class": "highlight"})

        def stringify_and_split(span: BeautifulSoup):
            text = str(span).strip()
            return text.split("(...)")

        i = 0
        while i < len(spans):
            span = spans[i]
            pivot = span.text

            left_context = span.previous_sibling
            if left_context:
                ellipsis_split = stringify_and_split(left_context)
                closest_left_text = ellipsis_split[-1]
            else:
                closest_left_text = ""

            right_context = span.next_sibling
            if right_context:
                ellipsis_split = stringify_and_split(right_context)
                closest_right_text = ellipsis_split[0]

                # check if gallica has made an erroneous (..) split in the middle of our pivot, only for requests with any multi-word terms
                if (
                    any(len(term.split(" ")) > 1 for term in record.terms)
                    and i < len(spans) - 1
                    and closest_right_text == ""
                ):
                    next_pivot = spans[i + 1].text
                    if any(
                        f'"{pivot} {next_pivot}"'.casefold() == term.casefold()
                        for term in record.terms
                    ):
                        pivot = f"{pivot} {next_pivot}"
                        new_right_context = spans[i + 1].next_sibling
                        if new_right_context:
                            ellipsis_split = stringify_and_split(new_right_context)
                            closest_right_text = ellipsis_split[0]

                        # ignore the next span
                        i += 1

            else:
                closest_right_text = ""
            i += 1
            rows.append(
                ContextRow(
                    pivot=pivot,
                    left_context=closest_left_text,
                    right_context=closest_right_text,
                    page_url=f"{record.url}/f{page.page_num}.image.r={pivot}",
                    page=page.page_num,
                )
            )

    return GallicaRecordWithRows(
        paper_title=record.paper_title,
        paper_code=record.paper_code,
        terms=record.terms,
        date=str(record.date),
        url=record.url,
        context=rows,
        ark=record.ark,
        author=record.author,
        ocr_quality=record.ocr_quality,
    )


async def get_occurrences_and_context(
    args: ContextSearchArgs,
    on_get_total_records: Callable[[int], None],
    on_get_origin_urls: Callable[[List[str]], None],
    session: aiohttp.ClientSession,
) -> Tuple[List[VolumeRecord], List[HTMLContext]]:
    """Queries Gallica's SRU and ContentSearch API's to get metadata and context for a given term in the archive."""

    link = None
    if args.link_distance and args.link_term:
        link = (args.link_term, args.link_distance)

    # get the volumes in which the term appears
    volume_Gallica_wrapper = VolumeOccurrence()
    gallica_records = await volume_Gallica_wrapper.get(
        terms=args.terms,
        start_date=make_date_from_year_mon_day(args.year, args.month, args.day),
        end_date=make_date_from_year_mon_day(args.end_year, args.end_month, args.day),
        codes=args.codes,
        source=args.source,
        link=link,
        limit=args.limit,
        start_index=args.cursor or 0,
        sort=args.sort,
        on_get_total_records=on_get_total_records,
        on_get_origin_urls=on_get_origin_urls,
        session=session,
    )

    gallica_records = list(gallica_records)

    # get the context for those volumes
    content_wrapper = Context()
    context = await content_wrapper.get(
        [(record.ark, record.terms) for record in gallica_records],
        session=session,
    )
    return gallica_records, list(context)


async def get_occurrences_use_ContentSearch(
    args: ContextSearchArgs,
    on_get_total_records: Callable[[int], None],
    on_get_origin_urls: Callable[[List[str]], None],
    session: aiohttp.ClientSession,
    parser: Callable = build_html_record,
) -> List[GallicaRecordWithHTML] | List[GallicaRecordWithRows]:
    """Queries Gallica's SRU and ContentSearch API's to get context for a given term in the archive."""

    records_with_context: List[GallicaRecordWithRows] | List[GallicaRecordWithHTML] = []
    documents, context = await get_occurrences_and_context(
        args=args,
        on_get_total_records=on_get_total_records,
        on_get_origin_urls=on_get_origin_urls,
        session=session,
    )
    keyed_documents = {record.ark: record for record in documents}
    for context_response in context:
        corresponding_record = keyed_documents[context_response.ark]
        records_with_context.append(parser(corresponding_record, context_response))

    return records_with_context


get_html_context = partial(get_occurrences_use_ContentSearch, parser=build_html_record)

get_row_context = partial(get_occurrences_use_ContentSearch, parser=build_row_record)


async def get_occurrences_use_RequestDigitalElement(
    args: ContextSearchArgs,
    on_get_total_records: Callable[[int], None],
    on_get_origin_urls: Callable[[List[str]], None],
    session: aiohttp.ClientSession,
) -> List[GallicaRecordWithPages]:
    """Queries Gallica's SRU, ContentSearch, and RequestDigitalElement API's to get metadata and page text for term occurrences."""

    documents, context = await get_occurrences_and_context(
        args=args,
        on_get_total_records=on_get_total_records,
        on_get_origin_urls=on_get_origin_urls,
        session=session,
    )

    # context will be filled with page text for each occurrence
    keyed_documents = {
        record.ark: GallicaRecordWithPages(
            paper_title=record.paper_title,
            paper_code=record.paper_code,
            terms=record.terms,
            date=str(record.date),
            url=record.url,
            ark=record.ark,
            author=record.author,
            ocr_quality=record.ocr_quality,
            context=[],
        )
        for record in documents
    }

    # semaphore limits the number of concurrent page requests
    sem = asyncio.Semaphore(10)
    page_text_wrapper = PageText()

    queries: List[PageQuery] = []
    for document_context in context:
        record = keyed_documents[document_context.ark]
        for page in document_context.pages:
            queries.append(
                PageQuery(
                    ark=record.ark,
                    page_num=int(page.page_num),
                )
            )

    # fetch the page text for each occurrence
    page_data = await page_text_wrapper.get(
        page_queries=queries,
        session=session,
        semaphore=sem,
    )
    for occurrence_page in page_data:
        record = keyed_documents[occurrence_page.ark]
        terms_string = " ".join(record.terms)
        record.context.append(
            {
                "page_num": occurrence_page.page_num,
                "text": occurrence_page.text,
                "page_url": f"{record.url}/f{occurrence_page.page_num}.image.r={terms_string}",
            }
        )

    return list(keyed_documents.values())


def make_date_from_year_mon_day(
    year: Optional[int], month: Optional[int], day: Optional[int]
) -> str:
    if year and month and day:
        return f"{year}-{month}-{day}"
    elif month:
        return f"{year}-{month}"
    elif year:
        return f"{year}"
    else:
        return ""
