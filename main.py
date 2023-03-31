import asyncio
from collections import Counter
import os
import aiohttp.client_exceptions
from bs4 import BeautifulSoup, ResultSet
import uvicorn
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
)
from gallicaGetter.context import Context, HTMLContext
from gallicaGetter.contextSnippets import ContextSnippets, ExtractRoot
from gallicaGetter.mostFrequent import get_gallica_core
from gallicaGetter.pageText import PageQuery, PageText
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
from gallicaGetter.queries import VolumeQuery
from gallicaGetter.volumeOccurrence import VolumeOccurrence, VolumeRecord
from gallicaGetter.utils.index_query_builds import get_num_results_for_queries
import psycopg

from models import (
    ContextRow,
    ContextSearchArgs,
    GallicaPageContext,
    GallicaRecordFullPageText,
    GallicaRowContext,
    MostFrequentRecord,
    UserResponse,
)

app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# More than 20-30 triggers a rate limit
MAX_CONCURRENT_REQUESTS = 10
MAX_CSV_RECORDS = 20000


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


async def stream_csv(args: ContextSearchArgs, id: int):
    link = None
    if args.link_distance and args.link_term:
        link_distance = int(args.link_distance)
        link = (args.link_term, link_distance)

    total_records = 0
    origin_urls = []

    def set_total_records(num_records: int):
        nonlocal total_records
        total_records = num_records

    def set_origin_urls(urls: List[str]):
        nonlocal origin_urls
        origin_urls = urls

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        volume_Gallica_wrapper = VolumeOccurrence()
        gallica_records = await volume_Gallica_wrapper.get(
            terms=args.terms,
            start_date=make_date_from_year_mon_day(args.year, args.month, 1),
            end_date=make_date_from_year_mon_day(args.end_year, args.end_month, 1),
            codes=args.codes,
            source=args.source,
            link=link,
            sort=args.sort,
            on_get_total_records=set_total_records,
            on_get_origin_urls=set_origin_urls,
            get_all_results=True,
            session=session,
            semaphore=semaphore,
        )

        # get the context for those volumes
        content_wrapper = Context()

        batch_of_num_workers: List[VolumeRecord] = []
        continue_loop = True

        while continue_loop:
            for _ in range(MAX_CONCURRENT_REQUESTS):
                try:
                    batch_of_num_workers.append(next(gallica_records))
                except StopIteration:
                    continue_loop = False
                    break
            code_dict = {record.ark: record for record in batch_of_num_workers}
            context = await content_wrapper.get(
                [
                    (record.url.split("/")[-1], record.terms)
                    for record in batch_of_num_workers
                ],
                session=session,
                semaphore=semaphore,
            )
            for context_response in context:
                record = code_dict[context_response.ark]
                rows = build_row_record_from_ContentSearch_response(
                    record, context_response
                )
                for context_row in rows:
                    yield (
                        id,
                        record.paper_title,
                        record.paper_code,
                        record.date,
                        context_row.page_url,
                        context_row.left_context,
                        context_row.pivot,
                        context_row.right_context,
                    )


async def download_csv_to_db(args: ContextSearchArgs, id: int):
    # iterate over stream_csv and write rows to db
    async with await psycopg.AsyncConnection(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        database=os.environ["POSTGRES_DB"],
    ) as conn:
        async with conn.cursor() as cur:
            async with cur.copy(
                f"copy csv (request_id, paper_title, paper_code, date, page_url, left_context, pivot, right_context) from stdin"
            ) as copy:
                async for row in stream_csv(args, id):
                    await copy.write_row(row)
            await conn.commit()


csv_id = 0


def get_csv_id():
    global csv_id
    csv_id += 1
    return csv_id


@app.post("/api/downloadCSV")
async def begin_download_csv(
    args: ContextSearchArgs, background_tasks: BackgroundTasks
):
    query = VolumeQuery(
        terms=args.terms,
        start_date=str(args.year) or "1000",
        end_date=str(args.end_year) or "2020",
        start_index=0,
        limit=1,
    )
    async with aiohttp.ClientSession() as session:
        num_records = await get_num_results_for_queries([query], session=session)
        num_records = int(num_records[0].gallica_results_for_params)
        if num_records > MAX_CSV_RECORDS:
            raise HTTPException(
                status_code=400,
                detail=f"Too many records to download ({num_records}). The current limit is {MAX_CSV_RECORDS}. Please narrow your search.",
            )
    id = get_csv_id()
    background_tasks.add_task(download_csv_to_db, args, id)
    return {"request_id": id}


@app.get("/api/topPapers")
async def top_papers(
    term: List[str] = Query(...),
    year: Optional[int] = None,
    month: Optional[int] = None,
    end_year: Optional[int] = None,
    end_month: Optional[int] = None,
):
    volume_wrapper = VolumeOccurrence()
    top_papers: Dict[str, int] = {}
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        try:
            volumes = await volume_wrapper.get(
                terms=term,
                start_date=make_date_from_year_mon_day(year=year, month=month, day=1),
                end_date=make_date_from_year_mon_day(
                    year=end_year, month=end_month, day=1
                ),
                session=session,
                semaphore=sem,
                get_all_results=True,
            )
            for volume in volumes:
                if volume.paper_title not in top_papers:
                    top_papers[volume.paper_title] = 0
                top_papers[volume.paper_title] += 1
            return Counter(top_papers).most_common(10)

        except aiohttp.client_exceptions.ClientConnectorError:
            return HTTPException(
                status_code=503, detail="Could not connect to Gallica."
            )


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
    all_context: Optional[bool] = False,
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

    try:
        async with aiohttp.ClientSession() as session:
            total_records = 0
            origin_urls = []

            def set_total_records(num_records: int):
                nonlocal total_records
                total_records = num_records

            def set_origin_urls(urls: List[str]):
                nonlocal origin_urls
                origin_urls = urls

            # build a semaphore to limit the number of concurrent requests to Gallica, this will be passed around as props
            sem = asyncio.Semaphore(10)

            keyed_docs = {
                record.ark: record
                for record in await get_documents_with_occurrences(
                    args=args,
                    on_get_total_records=set_total_records,
                    on_get_origin_urls=set_origin_urls,
                    session=session,
                    semaphore=sem,
                )
            }

            props = {
                "session": session,
                "keyed_docs": keyed_docs,
                "sem": sem,
            }

            if include_page_text and all_context:
                records = await get_context_include_full_page(
                    **props, context_source=get_all_context_in_documents
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
                records = [
                    record
                    async for record in get_context_parse_by_row(
                        **props,
                        context_source=get_sample_context_in_documents,
                        row_splitter=build_row_record_from_extract,
                    )
                ]
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
    semaphore: asyncio.Semaphore,
) -> List[VolumeRecord]:
    """Queries Gallica's SRU API to get metadata for a given term in the archive."""

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
        semaphore=semaphore,
    )

    return list(gallica_records)


async def get_context_parse_by_row(
    keyed_docs: Dict[str, VolumeRecord],
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    context_source: Callable,
    row_splitter: Callable,
):
    """Gets all occurrences of a term in a given time period, and returns a list of records for each occurrence."""

    for context_response in await context_source(
        list(keyed_docs.values()), session, sem
    ):
        record = keyed_docs[context_response.ark]
        page_rows = row_splitter(record, context_response)
        yield GallicaRowContext(**record.dict(), context=[row for row in page_rows])


async def get_raw_context(
    keyed_docs: Dict[str, VolumeRecord],
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    context_source: Callable,
):
    """Gets all occurrences of a term in a given time period, and returns a list of records for each occurrence."""

    for context_response in await context_source(
        records=list(keyed_docs.values()), session=session, semaphore=sem
    ):
        record = keyed_docs[context_response.ark]
        yield GallicaPageContext(
            **record.dict(), context=[page.context for page in context_response.pages]
        )


async def get_context_include_full_page(
    keyed_docs: Dict[str, VolumeRecord],
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
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
        records=list(keyed_docs.values()), session=session, semaphore=sem
    ):
        record = keyed_docs[context_response.ark]
        for page in context_response.pages:
            queries.append(
                PageQuery(
                    ark=record.ark,
                    page_num=int(page.page_num),
                )
            )
    page_data = await page_text_wrapper.get(
        page_queries=queries, semaphore=sem, session=session
    )
    for occurrence_page in page_data:
        record = gallica_records[occurrence_page.ark]
        terms_string = " ".join(record.terms)

        record.context.append(
            {
                "page_num": occurrence_page.page_num,
                "text": occurrence_page.text,
                "page_url": f"{record.url}/f{occurrence_page.page_num}.image.r={terms_string}",
            }
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
                yield row


def build_row_record_from_extract(record: VolumeRecord, extract: ExtractRoot):
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
                yield row


async def get_all_context_in_documents(
    records: List[VolumeRecord],
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
) -> List[HTMLContext]:
    """Queries Gallica's ContentSearch API's to get context for ALL occurrences within a list of documents."""

    context_wrapper = Context()
    context = await context_wrapper.get(
        [(record.ark, record.terms) for record in records],
        session=session,
        semaphore=semaphore,
    )
    return list(context)


async def get_sample_context_in_documents(
    records: List[VolumeRecord],
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
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
        semaphore=semaphore,
    )
    test = [i for i in range(20)] if 1 == 1 else 2
    return list(context)


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
