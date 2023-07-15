import asyncio
from contextlib import asynccontextmanager
import os
import aiohttp.client_exceptions
from bs4 import BeautifulSoup, ResultSet
from pydantic import BaseModel
import uvicorn
from typing import Any, Callable, Dict, List, Literal, Optional
from gallicaGetter.context import Context, HTMLContext
from gallicaGetter.contextSnippets import ContextSnippets, ExtractRoot
from gallicaGetter.fetch import APIRequest, fetch_queries_concurrently
from gallicaGetter.imageSnippet import ImageArgs, ImageSnippet
from gallicaGetter.pageText import PageQuery, PageText
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
from gallicaGetter.queries import VolumeQuery
from gallicaGetter.utils.parse_xml import (
    get_decollapsing_data_from_gallica_xml,
    get_num_records_from_gallica_xml,
    get_paper_title_from_record_xml,
    get_publisher_from_record_xml,
    get_records_from_xml,
)
from gallicaGetter.volumeOccurrence import VolumeOccurrence, VolumeRecord

from models import (
    ContextRow,
    ContextSearchArgs,
    GallicaImageContext,
    GallicaPageContext,
    GallicaRecordFullPageText,
    GallicaRowContext,
    OCRPage,
    OccurrenceArgs,
    Paper,
    TopCity,
    TopPaper,
    UserResponse,
)


MAX_PAPERS_TO_SEARCH = 600

gallica_session: aiohttp.ClientSession


@asynccontextmanager
async def gallica_session_lifespan(app: FastAPI):
    global gallica_session
    gallica_session = aiohttp.ClientSession()
    async with gallica_session:
        yield


def session():
    return gallica_session


app = FastAPI(lifespan=gallica_session_lifespan)
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# limit number of requests for routes... top_paper is more intensive


async def date_params(
    year: Optional[int] = 0,
    month: Optional[int] = 0,
    end_year: Optional[int] = 0,
    end_month: Optional[int] = 0,
):
    return {
        "start_date": make_date_from_year_mon_day(year=year, month=month),
        "end_date": make_date_from_year_mon_day(year=end_year, month=end_month),
    }


@app.get("/")
def index():
    return {"message": "ok"}


@app.get("/api/pageText")
async def page_text(ark: str, page: int):
    """Retrieve the full text of a document page on Gallica."""
    try:
        page_text_getter = PageText()
        page_data = await page_text_getter.get(
            page_queries=[PageQuery(ark=ark, page_num=page)],
            session=gallica_session,
        )
        page_data = list(page_data)
        if page_data and len(page_data) > 0:
            return page_data[0]
        return None
    except aiohttp.client_exceptions.ClientConnectorError:
        raise HTTPException(status_code=503, detail="Could not connect to Gallica.")


top_paper_lock = asyncio.Lock()


def get_lock():
    return top_paper_lock


@app.get("/api/top")
async def top(
    term: List[str] = Query(...),
    limit: int = 10,
    date_params: dict = Depends(date_params),
    lock: asyncio.Lock = Depends(get_lock),
    session: aiohttp.ClientSession = Depends(session),
):
    # have to lock this route because it's the most intensive on Gallica's servers...
    async with lock:
        try:
            top_papers: List[TopPaper] = []
            top_cities: List[TopCity] = []
            num_papers = 0
            num_results = 0
            query = VolumeQuery(
                terms=term,
                start_date=date_params["start_date"],
                end_date=date_params["end_date"],
                limit=1,
                collapsing=True,
                start_index=0,
                ocrquality=90,
                source="periodical",
                language="fre",
            )
            num_paper_response = await APIRequest(
                query=query,
                session=gallica_session,
                task_id=0,
                attempts_left=3,
                on_success=lambda: None,
            ).call_gallica_once()
            if num_paper_response is not None:
                num_papers = get_num_records_from_gallica_xml(num_paper_response.text)
                query.gallica_results_for_params = num_papers
                total_records_extra = get_decollapsing_data_from_gallica_xml(
                    num_paper_response.text
                )
                if total_records_extra and total_records_extra.isdigit():
                    num_results = int(total_records_extra)
            if num_results > 50000 and num_papers > 1000:
                raise HTTPException(
                    status_code=400,
                    detail=f"Too many results for a full scan ({num_results}). Current limit is 50000. Please narrow your search.",
                )
            # Check which counting method results in fewer requests to Gallica
            if num_papers < (num_results / 50):
                top_papers, top_cities = await sum_by_paper_query(
                    query=query, session=session, **date_params
                )
            else:
                query.collapsing = False
                query.gallica_results_for_params = num_results
                top_papers, top_cities = await sum_by_record(
                    query=query, session=session
                )

            def sort_by_count_and_return_top_limit(items):
                items.sort(key=lambda x: x.count, reverse=True)
                return items[:limit]

            return {
                "top_papers": sort_by_count_and_return_top_limit(top_papers),
                "top_cities": sort_by_count_and_return_top_limit(top_cities),
            }

        except aiohttp.client_exceptions.ClientConnectorError as e:
            return HTTPException(status_code=503, detail=e.strerror)


async def sum_by_paper_query(
    query: VolumeQuery,
    start_date: str,
    end_date: str,
    session: aiohttp.ClientSession,
):
    top_papers: List[TopPaper] = []
    city_counts: Dict[str, int] = {}
    occurrence_wrapper = VolumeOccurrence()
    paper_generator = await occurrence_wrapper.get_custom_query(query, session=session)
    num_results_in_paper_queries: List[VolumeQuery] = []
    for volume in paper_generator:
        num_results_in_paper_queries.append(
            VolumeQuery(
                terms=query.terms,
                start_date=start_date,
                end_date=end_date,
                limit=1,
                collapsing=False,
                start_index=0,
                codes=[volume.paper_code],
            )
        )
    for response in await fetch_queries_concurrently(
        queries=num_results_in_paper_queries,
        session=session,
    ):
        num_results = get_num_records_from_gallica_xml(xml=response.text)
        if num_results > 0:
            records = get_records_from_xml(response.text)
            record = records[0]
            if record is not None:
                top_papers.append(
                    TopPaper(
                        paper=Paper(
                            code=response.query.codes[0],
                            title=get_paper_title_from_record_xml(record),
                            publisher=get_publisher_from_record_xml(record),
                        ),
                        count=num_results,
                    )
                )
                record_publisher = get_publisher_from_record_xml(record)
                if record_publisher not in city_counts:
                    city_counts[record_publisher] = 0
                city_counts[record_publisher] += num_results

    return top_papers, [
        TopCity(city=city, count=count) for city, count in city_counts.items()
    ]


async def sum_by_record(
    query: VolumeQuery,
    session: aiohttp.ClientSession,
):
    volume_wrapper = VolumeOccurrence()
    top_papers: Dict[str, TopPaper] = {}
    top_cities: Dict[str, TopCity] = {}
    volume_generator = await volume_wrapper.get_custom_query(query, session=session)
    for volume in volume_generator:
        if volume.paper_title not in top_papers:
            top_papers[volume.paper_title] = TopPaper(
                count=1,
                paper=Paper(
                    code=volume.paper_code,
                    title=volume.paper_title,
                    publisher=volume.publisher,
                ),
            )
        top_papers[volume.paper_title].count += 1
        if volume.publisher:
            if volume.publisher not in top_cities:
                top_cities[volume.publisher] = TopCity(city=volume.publisher, count=0)
            top_cities[volume.publisher].count += 1
    return list(top_papers.values()), list(top_cities.values())


@app.get("/api/image")
async def image_snippet(ark: str, term: str, page: int):
    url = "https://rapportgallica.bnf.fr/api/snippet"
    headers = {"Content-Type": "application/json"}
    data = {
        "ark": ark,
        "isPeriodique": True,
        "pages": [page],
        "query": f'(gallica any "{term}")',
        "limitSnippets": 1,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=data) as resp:
            if resp.status != 200:
                return {"error": "error"}
            result = await resp.json()
            return {"image": result[0]["snippetBeans"][0]["content"]}


@app.get("/api/gallicaRecords/image")
async def fetch_records_with_images(
    terms: List[str] = Query(),
    codes: Optional[List[str]] = Query(None),
    cursor: Optional[int] = 0,
    date_params: dict = Depends(date_params),
    limit: Optional[int] = 10,
    link_term: Optional[str] = None,
    link_distance: Optional[int] = 0,
    source: Literal["book", "periodical", "all"] = "all",
    sort: Literal["date", "relevance"] = "relevance",
    session: aiohttp.ClientSession = Depends(session),
):
    args = ContextSearchArgs(
        start_date=date_params["start_date"],
        end_date=date_params["end_date"],
        terms=terms,
        codes=codes,
        cursor=cursor,
        limit=limit,
        link_term=link_term,
        link_distance=link_distance,
        source=source,
        sort=sort,
    )

    try:
        total_records = 0
        origin_urls = []

        def set_total_records(num_records: int):
            nonlocal total_records
            total_records = num_records

        def set_origin_urls(urls: List[str]):
            nonlocal origin_urls
            origin_urls = urls

        keyed_docs = {
            record.ark: record
            for record in await get_documents_with_occurrences(
                args=args,
                on_get_total_records=set_total_records,
                on_get_origin_urls=set_origin_urls,
                session=session,
            )
        }

        records = [
            record
            async for record in get_raw_context(
                keyed_docs=keyed_docs,
                session=session,
                context_source=get_sample_context_in_documents,
            )
        ]

        for record in records:
            record.context = [
                {"context": page.context, "page": page.page_num, "url": page.value.url}
                for page in record.context.pages
            ]

        image_wrapper = ImageSnippet()
        payloads: List[ImageArgs] = []
        for record in records:
            page = record.context[0]
            if page["page"]:
                payloads.append(
                    ImageArgs(
                        ark=record.ark,
                        page=page["page"],
                        term=record.terms[0],
                    )
                )

        images = [
            image
            for image in await image_wrapper.get(
                payloads=payloads,
                session=session,
            )
        ]

        response_items: Dict[str, GallicaImageContext] = {
            ark: GallicaImageContext(
                **record.dict(),
                context=[],
            )
            for ark, record in keyed_docs.items()
        }
        for image in images:
            if image.ark in response_items:
                response_items[image.ark].context.append(image)

        return {
            "num_results": total_records,
            "origin_urls": origin_urls,
            "records": list(response_items.values()),
        }

    except (
        aiohttp.client_exceptions.ClientConnectorError,
        aiohttp.client_exceptions.ClientConnectionError,
    ):
        raise HTTPException(status_code=503, detail="Could not connect to Gallica.")


@app.get("/api/gallicaRecords")
async def fetch_records_from_gallica(
    terms: List[str] = Query(),
    codes: Optional[List[str]] = Query(None),
    cursor: Optional[int] = 0,
    date_params: dict = Depends(date_params),
    limit: Optional[int] = 10,
    link_term: Optional[str] = None,
    link_distance: Optional[int] = 0,
    source: Literal["book", "periodical", "all"] = "all",
    sort: Literal["date", "relevance"] = "relevance",
    row_split: Optional[bool] = False,
    include_page_text: Optional[bool] = False,
    all_context: Optional[bool] = False,
    session: aiohttp.ClientSession = Depends(session),
):
    """API endpoint for the context table. To fetch multiple terms linked with OR in the Gallica CQL, pass multiple terms parameters: /api/gallicaRecords?terms=term1&terms=term2&terms=term3"""

    if limit and limit > 50:
        raise HTTPException(
            status_code=400,
            detail="Limit must be less than or equal to 50, the maximum number of records for one request to Gallica.",
        )

    try:
        total_records = 0
        origin_urls = []

        def set_total_records(num_records: int):
            nonlocal total_records
            total_records = num_records

        def set_origin_urls(urls: List[str]):
            nonlocal origin_urls
            origin_urls = urls

        keyed_docs = {
            record.ark: record
            for record in await get_documents_with_occurrences(
                args=ContextSearchArgs(
                    start_date=date_params["start_date"],
                    end_date=date_params["end_date"],
                    terms=terms,
                    codes=codes,
                    cursor=cursor,
                    limit=limit,
                    link_term=link_term,
                    link_distance=link_distance,
                    source=source,
                    sort=sort,
                ),
                on_get_total_records=set_total_records,
                on_get_origin_urls=set_origin_urls,
                session=session,
            )
        }

        props = {
            "session": gallica_session,
            "keyed_docs": keyed_docs,
        }

        if include_page_text and all_context:
            records = await get_context_include_full_page(
                **props,
                context_source=get_all_context_in_documents,
            )
        elif include_page_text:
            records = await get_context_include_full_page(
                **props, context_source=get_sample_context_in_documents
            )
        elif row_split and all_context:
            records = [
                record
                async for record in get_context_parse_by_row(
                    **props,
                    context_source=get_all_context_in_documents,
                    row_splitter=build_row_record_from_ContentSearch_response,
                )
            ]
        elif row_split:
            records = [record async for record in get_sample_context_by_row(**props)]
        elif all_context:
            records = [
                record
                async for record in get_raw_context(
                    **props, context_source=get_all_context_in_documents
                )
            ]
        else:
            records = [
                record
                async for record in get_raw_context(
                    **props, context_source=get_sample_context_in_documents
                )
            ]
        return UserResponse(
            records=records,
            num_results=total_records,
            origin_urls=origin_urls,
        )

    except (
        aiohttp.client_exceptions.ClientConnectorError,
        aiohttp.client_exceptions.ClientConnectionError,
    ):
        raise HTTPException(status_code=503, detail="Could not connect to Gallica.")


async def get_documents_with_occurrences(
    args: ContextSearchArgs,
    on_get_total_records: Callable[[int], None],
    on_get_origin_urls: Callable[[List[str]], None],
    session: aiohttp.ClientSession,
) -> List[VolumeRecord]:
    """Queries Gallica's SRU API to get metadata for a given term in the archive."""

    link = None
    if args.link_distance and args.link_term:
        link = (args.link_term, args.link_distance)

    # get the volumes in which the term appears
    volume_Gallica_wrapper = VolumeOccurrence()
    gallica_records = await volume_Gallica_wrapper.get(
        args=OccurrenceArgs(
            terms=args.terms,
            start_date=args.start_date,
            end_date=args.end_date,
            codes=args.codes,
            source=args.source,
            link_distance=args.link_distance,
            link_term=args.link_term,
            limit=args.limit,
            start_index=args.cursor or 0,
            sort=args.sort,
        ),
        on_get_total_records=on_get_total_records,
        on_get_origin_urls=on_get_origin_urls,
        session=session,
    )

    return list(gallica_records)


def get_sample_context_by_row(
    keyed_docs: Dict[str, VolumeRecord],
    session: aiohttp.ClientSession,
):
    return get_context_parse_by_row(
        session=session,
        keyed_docs=keyed_docs,
        context_source=get_sample_context_in_documents,
        row_splitter=parse_rows_from_sample_context,
    )


async def get_context_parse_by_row(
    keyed_docs: Dict[str, VolumeRecord],
    session: aiohttp.ClientSession,
    context_source: Callable,
    row_splitter: Callable,
):
    for context_response in await context_source(list(keyed_docs.values()), session):
        record = keyed_docs[context_response.ark]
        page_rows = row_splitter(record, context_response)
        yield GallicaRowContext(**record.dict(), context=[row for row in page_rows])


async def get_raw_context(
    keyed_docs: Dict[str, VolumeRecord],
    session: aiohttp.ClientSession,
    context_source: Callable,
):
    for context_response in await context_source(
        records=list(keyed_docs.values()), session=session
    ):
        record = keyed_docs[context_response.ark]
        yield GallicaPageContext(**record.dict(), context=context_response)


async def get_context_include_full_page(
    keyed_docs: Dict[str, VolumeRecord],
    session: aiohttp.ClientSession,
    context_source: Callable,
):
    """Queries Context and PageText to get the text of each page a term occurs on."""
    page_text_wrapper = PageText()
    queries: List[PageQuery] = []

    # build records to be filled with page text for each page w/occurrence
    gallica_records: Dict[str, GallicaRecordFullPageText] = {
        record.ark: GallicaRecordFullPageText(**record.dict(), context=[])
        for record in keyed_docs.values()
    }

    for context_response in await context_source(
        records=list(keyed_docs.values()), session=session
    ):
        record = keyed_docs[context_response.ark]
        for page in context_response.pages:
            if page.page_num is None:
                continue
            queries.append(
                PageQuery(
                    ark=record.ark,
                    page_num=int(page.page_num),
                )
            )
    page_data = await page_text_wrapper.get(page_queries=queries, session=session)
    for occurrence_page in page_data:
        record = gallica_records[occurrence_page.ark]
        terms_string = " ".join(record.terms)

        record.context.append(
            OCRPage(
                page_num=occurrence_page.page_num,
                text=occurrence_page.text,
                page_url=f"{record.url}/f{occurrence_page.page_num}.image.r={terms_string}",
            )
        )
    return list(gallica_records.values())


def parse_spans_to_rows(spans: ResultSet[Any], record: VolumeRecord):
    """Gallica returns a blob of context for each page, this function splits the blob into a row for each occurrence."""
    rows: List[ContextRow] = []

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
            )
        )
    return rows


def build_row_record_from_ContentSearch_response(
    record: VolumeRecord, context: HTMLContext
):
    for page in context.pages:
        soup = BeautifulSoup(page.context, "html.parser")
        spans = soup.find_all("span", {"class": "highlight"})
        if spans:
            page_rows = parse_spans_to_rows(
                spans=spans,
                record=record,
            )
            for row in page_rows:
                row.page_url = f"{record.url}/f{page.page_num}.image.r={row.pivot}"
                row.page_num = page.page_num
                yield row


def parse_rows_from_sample_context(record: VolumeRecord, extract: ExtractRoot):
    """Split the Gallica HTML context on the highlighted spans, creating rows of pivot (span), left context, and right context."""
    # last element is a label, not a context extract
    snippets = extract.fragment.contenu[:-1]

    for snippet in snippets:
        text = snippet.value.contenu
        soup = BeautifulSoup(text, "html.parser")
        spans = soup.find_all("mark")
        if spans:
            page_rows = parse_spans_to_rows(spans, record)
            for row in page_rows:
                row.page_url = snippet.value.url
                row.page_num = snippet.value.page_num
                yield row


async def get_all_context_in_documents(
    records: List[VolumeRecord],
    session: aiohttp.ClientSession,
) -> List[HTMLContext]:
    """Queries Gallica's ContentSearch API's to get context for ALL occurrences within a list of documents."""

    context_wrapper = Context()
    context = await context_wrapper.get(
        [(record.ark, record.terms) for record in records],
        session=session,
    )
    return list(context)


async def get_sample_context_in_documents(
    records: List[VolumeRecord],
    session: aiohttp.ClientSession,
) -> List[ExtractRoot]:
    """Queries Gallica's search result API to show a sample of context instead of the entire batch."""

    # warn if terms length is greater than 1
    if any(len(record.terms) > 1 for record in records):
        print(
            "Warning: using sample context for multi-word terms; only the first term will be used."
        )
    context_snippet_wrapper = ContextSnippets()
    context = await context_snippet_wrapper.get(
        [(record.ark, record.terms[0]) for record in records],
        session=session,
    )

    return list(context)


def make_date_from_year_mon_day(
    year: Optional[int], month: Optional[int], day: Optional[int] | None = None
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
