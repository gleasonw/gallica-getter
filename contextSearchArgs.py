from typing import List, Literal, Optional
from pydantic import BaseModel


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
