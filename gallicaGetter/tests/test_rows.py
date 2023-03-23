from gallicaGetter.utils.date import Date
from gallicaContextSearch import build_row_record
from gallicaGetter.volumeOccurrence import VolumeRecord
from gallicaGetter.context import Context, HTMLContext, GallicaPage
import pytest

# https://gallica-grapher-production.up.railway.app/api/gallicaRecords?terms=histoire%20de%20la%20r%C3%A9volution&source=book&sort=relevance&year=1801&row_split=true&cursor=6&limit=1


@pytest.mark.asyncio
async def test_build_row_record():
    record = VolumeRecord(
        paper_title="Test",
        paper_code="Test",
        terms=['"histoire de la révolution"'],
        url="Test",
        date=Date("1000"),
    )
    broken_page = GallicaPage(
        page_label="PAG_150",
        context="On verra bient&#244;t combien il acquit de .c&#233;l&#233;brit&#233; dans l&apos;<span class='highlight'>histoire</span>(...)<span class='highlight'>de la r&#233;volution</span> de 1787",
    )
    normal_page = GallicaPage(
        page_label="PAG_238",
        context="Ces r&#233;volutions, dans les magistratures, &#233;taient, de grands &#233;v&#233;nemens dans l&apos;<span class='highlight'>histoire de la r&#233;volution</span> totale, qui elle",
    )
    context = HTMLContext(
        pages=[broken_page, normal_page],
        num_results=2,
        ark="Test",
    )
    row_record = build_row_record(record, context)
    assert (
        row_record.context[0].left_context
        == "On verra bientôt combien il acquit de .célébrité dans l'"
    )
    assert row_record.context[0].pivot == "histoire de la révolution"
    assert row_record.context[0].right_context == "de 1787"
    assert row_record.context[0].page == "150"
    assert (
        row_record.context[0].page_url == "Test/f150.image.r=histoire de la révolution"
    )
    assert (
        row_record.context[1].left_context
        == "Ces révolutions, dans les magistratures, étaient, de grands événemens dans l'"
    )
    assert row_record.context[1].right_context == "totale, qui elle"
    assert row_record.context[1].page == "238"
    assert (
        row_record.context[1].page_url == "Test/f238.image.r=histoire de la révolution"
    )
