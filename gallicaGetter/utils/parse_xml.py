from lxml import etree
from gallicaGetter.utils.date import Date
from typing import List, Tuple


def get_one_paper_from_record_batch(xml: bytes) -> str:
    records = get_records_from_xml(xml)
    if records:
        return get_paper_title_from_record_xml(records[0])
    else:
        return ""


def get_num_results_and_pages_for_context(
    xml: bytes,
) -> Tuple[int, List[Tuple[str, str]]]:
    try:
        elements = etree.fromstring(xml, parser=etree.XMLParser(encoding="utf-8"))
    except etree.XMLSyntaxError:
        return 0, []
    top_level = elements.find(".")
    num_results = top_level.attrib.get("countResults")
    items = top_level[1].findall("item")
    pages = get_page_and_context_for_occurrence(items)
    return num_results, pages


def get_records_from_xml(xml: bytes):
    try:
        elements = etree.fromstring(xml, parser=etree.XMLParser(encoding="utf-8"))
    except etree.XMLSyntaxError:
        return []
    records_root = elements.find("{http://www.loc.gov/zing/srw/}records")
    if records_root is None:
        return []
    return records_root.findall("{http://www.loc.gov/zing/srw/}record")


def get_html(xml: bytes) -> str:
    elements = etree.fromstring(xml, parser=etree.XMLParser(encoding="utf-8"))
    return elements.find("html").text


def get_num_records_from_gallica_xml(xml: bytes) -> int:
    xml_root = etree.fromstring(xml, parser=etree.XMLParser(encoding="utf-8"))
    num_results = xml_root.find("{http://www.loc.gov/zing/srw/}numberOfRecords")
    if num_results is not None:
        return int(num_results.text)
    else:
        return 0


def get_years_published(xml: str) -> List[str]:
    root = etree.fromstring(xml, parser=etree.XMLParser(encoding="utf-8"))
    years = [get_year_from_element(yearElement) for yearElement in root.iter("year")]
    return list(filter(None, years))


def get_year_from_element(year_element) -> str | None:
    if year_element is not None:
        year = year_element.text
        return year if year.isdigit() else None
    else:
        return None


def get_data_from_record_root(root):
    root = root[2]
    data = root[0]
    return data


def get_paper_code_from_record_xml(record) -> str:
    xml = get_data_from_record_root(record)
    code_element = xml.find("{http://purl.org/dc/elements/1.1/}relation")
    if code_element is not None:
        text = code_element.text
        paper_code = text[-11 : len(text)]
        return paper_code
    else:
        return "None"


def get_url_from_record(record) -> str:
    xml = get_data_from_record_root(record)
    url_element = xml.find("{http://purl.org/dc/elements/1.1/}identifier")
    if url_element is not None:
        url = url_element.text
        return url
    return ""


def get_paper_title_from_record_xml(record) -> str:
    xml = get_data_from_record_root(record)
    paper_title = xml.find("{http://purl.org/dc/elements/1.1/}title").text
    return paper_title


def get_date_from_record_xml(record) -> Date:
    xml = get_data_from_record_root(record)
    date_element = xml.find("{http://purl.org/dc/elements/1.1/}date")
    if date_element is not None:
        return Date(date_element.text)
    return Date("")


def get_author_from_record_xml(record) -> str:
    xml = get_data_from_record_root(record)
    author_element = xml.find("{http://purl.org/dc/elements/1.1/}creator")
    if author_element is not None:
        return author_element.text
    return ""


def get_ocr_quality_from_record_xml(record) -> str:
    extra_record_xml = record[-1]
    ocr_quality_element = extra_record_xml.find(".//nqamoyen")
    if ocr_quality_element is not None:
        return ocr_quality_element.text
    return ""


def get_page_and_context_for_occurrence(xml_items) -> List[Tuple[str, str]]:
    items = []
    for item in xml_items:
        page = item.find("p_id")
        page = page.text if page is not None else None
        content = item.find("content")
        content = content.text if content is not None else None
        items.append((page, content))
    return items
