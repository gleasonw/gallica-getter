import asyncio
from collections import Counter
import os
import aiohttp.client_exceptions
from bs4 import BeautifulSoup
import uvicorn
from typing import Callable, List, Literal, Optional, Tuple
from gallicaGetter.context import Context, HTMLContext
from gallicaGetter.mostFrequent import get_gallica_core
from gallicaGetter.pageText import PageQuery, PageText
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
from gallicaGetter.volumeOccurrence import VolumeOccurrence, VolumeRecord

from models import ContextRow, ContextSearchArgs, GallicaRecordWithHTML, GallicaRecordWithPages, GallicaRecordWithRows, MostFrequentRecord, UserResponse

app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# More than 40-50 triggers a rate limit
MAX_CONCURRENT_REQUESTS_TO_GALLICA = 20


@app.get("/")
def index():
    return {"message": "ok"}


@app.get("/api/pageText")
async def page_text(ark: str, page: int):
    """Retrieve the full text of a document page on Gallica."""
    try:
        async with aiohttp.ClientSession() as session:
            page_text_getter = PageText()
            page_data = await page_text_getter.get(
                page_queries=[PageQuery(ark=ark, page_num=page)],
                session=session,
            )
            page_data = list(page_data)
            if page_data and len(page_data) > 0:
                return page_data[0]
            return None
    except aiohttp.client_exceptions.ClientConnectorError:
        raise HTTPException(status_code=503, detail="Could not connect to Gallica.")


@app.get("/api/mostFrequentTerms")
async def most_frequent_terms(
    root_gram: str,
    sample_size: int,
    top_n: int = 10,
    start_year: Optional[int] = None,
    start_month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None,
):
    if sample_size and sample_size > 50:
        sample_size = 50
    async with aiohttp.ClientSession() as session:
        try:
            counts = await get_gallica_core(
                root_gram=root_gram,
                start_date=make_date_from_year_mon_day(
                    year=start_year, month=start_month, day=1
                ),
                end_date=make_date_from_year_mon_day(
                    year=end_year, month=end_month, day=1
                ),
                sample_size=sample_size,
                session=session,
            )
        except aiohttp.client_exceptions.ClientConnectorError:
            return HTTPException(
                status_code=503, detail="Could not connect to Gallica."
            )
    top = Counter(counts).most_common(top_n)
    return [MostFrequentRecord(term=term, count=count) for term, count in top]


@app.get("/api/gallicaRecords")
async def fetch_records_from_gallica(
    year: Optional[int] = 0,
    month: Optional[int] = 0,
    end_year: Optional[int] = 0,
    end_month: Optional[int] = 0,
    terms: List[str] = Query(),
    codes: Optional[List[str]] = Query(None),
    cursor: Optional[int] = 0,
    limit: Optional[int] = 10,
    link_term: Optional[str] = None,
    link_distance: Optional[int] = 0,
    source: Literal["book", "periodical", "all"] = "all",
    sort: Literal["date", "relevance"] = "relevance",
    row_split: Optional[bool] = False,
    include_page_text: Optional[bool] = False,
):
    """API endpoint for the context table. To fetch multiple terms linked with OR in the Gallica CQL, pass multiple terms parameters: /api/gallicaRecords?terms=term1&terms=term2&terms=term3"""

    # ensure multi-word terms are wrapped in quotes for an exact search in Gallica; don't double wrap though
    wrapped_terms = []
    for term in terms:
        if term.startswith('"') and term.endswith('"'):
            wrapped_terms.append(term)
        else:
            if " " in term:
                wrapped_terms.append(f'"{term}"')
            else:
                wrapped_terms.append(term)

    args = ContextSearchArgs(
        year=year,
        month=month,
        end_year=end_year,
        end_month=end_month,
        terms=wrapped_terms,
        codes=codes,
        cursor=cursor,
        limit=limit,
        link_term=link_term,
        link_distance=link_distance,
        source=source,
        sort=sort,
    )

    if limit and limit > 50:
        raise HTTPException(
            status_code=400,
            detail="Limit must be less than or equal to 50, the maximum number of records for one request to Gallica.",
        )

    async with aiohttp.ClientSession() as session:
        total_records = 0
        origin_urls = []

        def set_total_records(num_records: int):
            nonlocal total_records
            total_records = num_records

        def set_origin_urls(urls: List[str]):
            nonlocal origin_urls
            origin_urls = urls

        props = {
            "args": args,
            "session": session,
            "on_get_origin_urls": set_origin_urls,
            "on_get_total_records": set_total_records,
        }

        try:
            if row_split:
                records = await get_occurrences_use_ContentSearch(
                    **props, parser=build_row_record
                )
            elif include_page_text:
                records = await get_occurrences_use_RequestDigitalElement(**props)
            else:
                records = await get_occurrences_use_ContentSearch(
                    **props, parser=build_html_record
                )
            return UserResponse(
                records=records,
                num_results=total_records,
                origin_urls=origin_urls,
            )
        except aiohttp.client_exceptions.ClientConnectorError:
            raise HTTPException(status_code=503, detail="Could not connect to Gallica.")


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


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
