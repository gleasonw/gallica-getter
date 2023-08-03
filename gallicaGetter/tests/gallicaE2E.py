import aiohttp
import pytest
from gallicaGetter.pagination import Pagination
from gallicaGetter.context import Context
from gallicaGetter.issues import Issues
from gallicaGetter.papers import Papers
from gallicaGetter.queries import ContentQuery

from gallicaGetter.volumeOccurrence import VolumeOccurrence
from gallicaGetter.periodOccurrence import PeriodOccurrence
from gallicaGetter.pageText import PageQuery, PageText
from models import OccurrenceArgs


@pytest.mark.asyncio
async def test_pagination():
    async with aiohttp.ClientSession() as session:
        records = Pagination.get("bpt6k607811b", session=session)
        list_records = [record async for record in records]
        first = list_records[0]
        assert first.ark == "bpt6k607811b"
        assert first.page_count == 4
        assert first.has_content == True
        assert first.has_toc == False


@pytest.mark.asyncio
async def test_get_page():
    async with aiohttp.ClientSession() as session:
        records = PageText.get(
            page_queries=[PageQuery(ark="bpt6k607811b", page_num=1)], session=session
        )
        list_records = [record async for record in records]
        assert len(list_records) == 1
        first_record = list_records[0]
        assert first_record.ark == "bpt6k607811b"
        assert first_record.page_num == 1
        assert type(first_record.text) == str


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("input", "expected_length"),
    [
        ({"terms": ["seattle"], "codes": ["cb32895690j"]}, 268),
        (
            {
                "terms": ["bon pain"],
                "start_date": "1890-01-02",
                "end_date": "1890-01-03",
            },
            1,
        ),
        ({"terms": ["vote des femmes"], "start_date": "1848", "end_date": "1848"}, 1),
    ],
)
async def test_get_volume_occurrences(input, expected_length):
    records = await VolumeOccurrence.get(OccurrenceArgs(**input), get_all_results=True)
    list_records = list(records)
    assert len(list_records) == expected_length


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("input", "expected_length"),
    [
        (
            {
                "terms": ["bon pain"],
                "start_date": "1890",
                "end_date": "1895",
                "grouping": "year",
            },
            6,
        ),
        (
            {
                "terms": ["bon pain"],
                "start_date": "1890",
                "end_date": "1890",
                "grouping": "month",
            },
            12,
        ),
    ],
)
async def test_get_period_occurrences(input, expected_length):
    async with aiohttp.ClientSession() as session:
        records = PeriodOccurrence.get(
            OccurrenceArgs(**input), grouping=input["grouping"], session=session
        )
        list_records = [record async for record in records]
        assert len(list_records) == expected_length


@pytest.mark.asyncio
async def test_get_issues():
    async with aiohttp.ClientSession() as session:
        records = Issues.get("cb344484501", session=session)
        list_records = [record async for record in records]
        assert len(list_records) == 1
        issue = list_records[0]
        assert issue.code == "cb344484501"


@pytest.mark.asyncio
async def test_get_content():
    async with aiohttp.ClientSession() as session:
        records = Context.get(
            queries=[ContentQuery(ark="bpt6k267221f", terms=["erratum"])],
            session=session,
        )
        list_records = [record async for record in records]
        context = list_records[0]
        assert context.ark == "bpt6k267221f"


@pytest.mark.asyncio
async def test_get_papers_wrapper():
    papers = await Papers.get(["cb32895690j"])
    paper = list(papers)[0]
    assert paper.code == "cb32895690j"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
