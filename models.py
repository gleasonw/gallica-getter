from pydantic import BaseModel
from typing import List, Optional, Dict, Literal


class Paper(BaseModel):
    code: str
    title: str
    publisher: Optional[str]


class TopPaper(BaseModel):
    count: int
    paper: Paper


class TopPaperResponse(BaseModel):
    num_results: int
    original_query: str
    top_papers: List[TopPaper]


class ContextRow(BaseModel):
    pivot: str
    left_context: str
    right_context: str
    page_url: Optional[str | None] = None


class GallicaRecordBase(BaseModel):
    paper_title: str
    paper_code: str
    ark: str
    terms: List[str]
    date: str
    url: str
    author: str
    ocr_quality: float


class GallicaRowContext(GallicaRecordBase):
    context: List[ContextRow]


class GallicaPageContext(GallicaRecordBase):
    context: List[str]


class GallicaRecordFullPageText(GallicaRecordBase):
    context: List[Dict[str, str | int]]


class UserResponse(BaseModel):
    records: List[GallicaRowContext] | List[GallicaPageContext] | List[
        GallicaRecordFullPageText
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
