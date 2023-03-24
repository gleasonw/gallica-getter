from pydantic import BaseModel
from typing import List, Optional, Dict, Literal

from gallicaGetter.context import HTMLContext


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


class UserResponse(BaseModel):
    records: List[GallicaRecordWithRows] | List[GallicaRecordWithHTML] | List[
        GallicaRecordWithPages
    ]
    num_results: int
    origin_urls: List[str]


class ContextSearchArgs(BaseModel):
    terms: List[str]
    codes: Optional[List[str]] = None
    year: Optional[int] = 0
    month: Optional[int] = 0
    end_year: Optional[int] = 0
    end_month: Optional[int] = 0
    day: Optional[int] = 0
    cursor: Optional[int] = 0
    limit: Optional[int] = 10
    link_term: Optional[str] = None
    link_distance: Optional[int] = 0
    source: Literal["book", "periodical", "all"] = "all"
    sort: Literal["date", "relevance"] = "relevance"


class MostFrequentRecord(BaseModel):
    term: str
    count: int
