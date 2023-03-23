from collections import Counter
import os
import aiohttp.client_exceptions
import uvicorn
from typing import List, Literal, Optional
from pydantic import BaseModel
from gallicaGetter.mostFrequent import get_gallica_core
from gallicaGetter.pageText import PageQuery, PageText
from gallicaContextSearch import (
    GallicaRecordWithHTML,
    GallicaRecordWithPages,
    GallicaRecordWithRows,
    get_occurrences_use_RequestDigitalElement,
    get_row_context,
    get_html_context,
    make_date_from_year_mon_day,
)
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import aiohttp
from contextSearchArgs import ContextSearchArgs

app = FastAPI()

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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


class MostFrequentRecord(BaseModel):
    term: str
    count: int


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


class UserResponse(BaseModel):
    records: List[GallicaRecordWithRows] | List[GallicaRecordWithHTML] | List[
        GallicaRecordWithPages
    ]
    num_results: int
    origin_urls: List[str]


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
                records = await get_row_context(**props)
            elif include_page_text:
                records = await get_occurrences_use_RequestDigitalElement(**props)
            else:
                records = await get_html_context(**props)
            return UserResponse(
                records=records,
                num_results=total_records,
                origin_urls=origin_urls,
            )
        except aiohttp.client_exceptions.ClientConnectorError:
            raise HTTPException(status_code=503, detail="Could not connect to Gallica.")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
