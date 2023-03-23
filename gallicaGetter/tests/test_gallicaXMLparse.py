from unittest import TestCase
from gallicaGetter.utils.parse_xml import *
from gallicaGetter.utils.date import Date
import os

here = os.path.dirname(os.path.abspath(__file__))


class TestGallicaXMLparse(TestCase):
    def setUp(self) -> None:
        with open(os.path.join(here, "xmlForTesting/occurrences.xml"), "rb") as f:
            self.occurrencesXML = f.read()
        with open(os.path.join(here, "xmlForTesting/papers.xml"), "rb") as f:
            self.papersXML = f.read()
        with open(os.path.join(here, "xmlForTesting/issues.xml"), "rb") as f:
            self.issuesXML = f.read()
        with open(os.path.join(here, "xmlForTesting/ocrText.xml"), "rb") as f:
            self.ocrTextXML = f.read()

    def test_get_one_paper_from_record_batch(self):
        self.assertEqual(
            get_one_paper_from_record_batch(self.occurrencesXML),
            "Revue maritime et coloniale / Ministère de la marine et des colonies",
        )

    def test_get_num_results_and_pages_for_occurrence_in_periodical(self):
        countAndResults = get_num_results_and_pages_for_context(self.ocrTextXML)
        self.assertEqual(countAndResults[0], "364")
        self.assertEqual(len(countAndResults[1]), 5)

    def test_get_records_from_xml(self):
        test = get_records_from_xml(self.occurrencesXML)
        self.assertEqual(len(test), 6)

    def test_get_num_records(self):
        self.assertEqual(get_num_records_from_gallica_xml(self.occurrencesXML), 78514)

    def test_get_years_published(self):
        self.assertEqual(
            get_years_published(self.issuesXML),
            list(str(year) for year in range(1861, 1943)),
        )

    def test_get_paper_code(self):
        records = get_records_from_xml(self.papersXML)
        oneRecord = records[0]
        self.assertEqual(get_paper_code_from_record_xml(oneRecord), "cb32699739p")

    def test_get_url(self):
        records = get_records_from_xml(self.occurrencesXML)
        oneRecord = records[0]
        self.assertEqual(
            get_url_from_record(oneRecord),
            "https://gallica.bnf.fr/ark:/12148/bpt6k34565h",
        )

    def test_get_paper_title(self):
        records = get_records_from_xml(self.papersXML)
        oneRecord = records[0]
        self.assertEqual(
            get_paper_title_from_record_xml(oneRecord),
            "L'Anti-cafard. Revue anti-boche, publiée très irrégulièrement avec le concours de toutes les bonnes volontés",
        )

    def test_get_date(self):
        records = get_records_from_xml(self.occurrencesXML)
        oneRecord = records[0]
        self.assertEqual(
            get_date_from_record_xml(oneRecord).year,
            Date("1883-04").year,
        )
