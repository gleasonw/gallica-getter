from dataclasses import dataclass
from typing import List, Literal, Optional, Tuple
from pydantic import BaseModel


class VolumeQuery(BaseModel):
    """Struct for a query to Gallica's SRU API."""

    terms: List[str]
    start_date: Optional[str]
    end_date: Optional[str]
    start_index: int
    limit: int
    link: Optional[Tuple[str, int]] = None
    source: Optional[Literal["book", "periodical", "all"]] = "all"
    codes: Optional[List[str]] = None
    sort: Optional[Literal["date", "relevance"]] = None
    gallica_results_for_params: int = 0
    collapsing = False

    @property
    def endpoint_url(self):
        return "https://gallica.bnf.fr/SRU"

    @property
    def params(self):
        base = {
            "operation": "searchRetrieve",
            "exactSearch": "True",
            "version": 1.2,
            "startRecord": self.start_index,
            "maximumRecords": self.limit,
            "query": self.build_cql_string(),
            "collapsing": self.collapsing and "true" or "false",
        }
        return base

    def make_copy(self, start_index: int, num_records: int = 1):
        return VolumeQuery(
            terms=self.terms,
            codes=self.codes,
            start_date=self.start_date,
            end_date=self.end_date,
            start_index=start_index,
            limit=num_records,
            link=self.link,
            source=self.source,
            sort=self.sort,
        )

    def build_cql_string(self):
        cql_components = []
        if termCQL := self.build_gram_cql():
            cql_components.append(termCQL)
        if dateCQL := self.build_date_cql():
            cql_components.append(dateCQL)
        if paperCQL := self.build_source_sql():
            cql_components.append(paperCQL)
        cql = " and ".join(cql_components)
        if self.sort == "date":
            cql += " sortby dc.date/sort.ascending"
        return cql

    def build_date_cql(self):
        if self.start_date and self.end_date and self.start_date != self.end_date:
            return f'gallicapublication_date>="{self.start_date}" and gallicapublication_date<"{self.end_date}"'
        elif self.start_date:
            return f'gallicapublication_date="{self.start_date}"'
        else:
            return ""

    def build_gram_cql(self) -> str:
        if self.link and len(self.terms) == 1:
            return f'text adj "{self.terms[0]}" prox/unit=word/distance={self.link[1]} "{self.link[0]}"'
        elif self.terms:
            return 'text adj "' + '" or text adj "'.join(self.terms) + '"'
        else:
            return ""

    def build_source_sql(self):
        if self.codes and self.codes[0]:
            formatted_codes = [f"{code}_date" for code in self.codes]
            return 'arkPress adj "' + '" or arkPress adj "'.join(formatted_codes) + '"'
        elif self.source == "periodical":
            base = 'dc.type all "fascicule"'
        elif self.source == "book":
            base = 'dc.type all "monographie"'
        else:
            base = 'dc.type all "fascicule" or dc.type all "monographie"'
        return base + ' and ocr.quality all "Texte disponible"'

    def __repr__(self):
        return f"Volume Query ({self.terms})"


@dataclass(slots=True)
class PaperQuery:
    """Struct for a paper metadata query to Gallica's SRU API. Similar to VolumeQuery, but with fewer params and a different CQL build."""

    start_index: int
    limit: int
    codes: Optional[List[str]] = None
    cql: Optional[str] = None
    gallica_results_for_params: int = 0

    @property
    def endpoint_url(self):
        return "https://gallica.bnf.fr/SRU"

    def __post_init__(self):
        if self.codes and self.codes[0]:
            formatted_codes = [f"{code}_date" for code in self.codes]
            self.cql = (
                'arkPress adj "' + '" or arkPress adj "'.join(formatted_codes) + '"'
            )
        else:
            self.cql = 'dc.type all "fascicule" and ocr.quality all "Texte disponible"'

    def make_copy(self, start_index: int, num_records: int):
        return PaperQuery(start_index, num_records, self.codes)

    @property
    def params(self):
        base = {
            "operation": "searchRetrieve",
            "exactSearch": "True",
            "version": 1.2,
            "startRecord": self.start_index,
            "maximumRecords": self.limit,
            "query": self.cql,
            "collapsing": "true",
        }
        return base


@dataclass(frozen=True, slots=True)
class IssuesQuery:
    """Struct for query to Gallica's Issues API."""

    code: str

    @property
    def endpoint_url(self):
        return "https://gallica.bnf.fr/services/Issues"

    @property
    def params(self):
        return {"ark": f"ark:/12148/{self.code}/date"}


@dataclass(frozen=True, slots=True)
class FullTextQuery:
    """Struct for a query to Gallica's full text API. The endpoint does not use query parameters, so the endpoint URL changes for each query."""

    ark: str

    @property
    def params(self):
        return {}

    @property
    def endpoint_url(self):
        return f"https://gallica.bnf.fr/ark:/12148/{self.ark}.texteBrut"

    def __repr__(self) -> str:
        return f"RawTextQuery({self.ark})"


@dataclass(frozen=True, slots=True)
class ContentQuery:
    """Struct for query to Gallica's ContentSearch API."""

    ark: str
    terms: List[str]

    @property
    def endpoint_url(self):
        return "https://gallica.bnf.fr/services/ContentSearch"

    @property
    def params(self):
        return {"ark": self.ark, "query": self.terms}
