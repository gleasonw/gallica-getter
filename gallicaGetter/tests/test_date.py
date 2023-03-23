from unittest import TestCase
from gallicaGetter.utils.date import Date


class TestDate(TestCase):
    def setUp(self) -> None:
        self.fullDate = Date("2020-01-01")
        self.yearDate = Date("2020")
        self.monthDate = Date("2020-01")

    def test_get_year(self):
        self.assertEqual(self.fullDate.year, "2020")
        self.assertEqual(self.yearDate.year, "2020")
        self.assertEqual(self.monthDate.year, "2020")

    def test_get_month(self):
        self.assertEqual(self.fullDate.month, "01")
        self.assertEqual(self.yearDate.month, None)
        self.assertEqual(self.monthDate.month, "01")

    def test_get_day(self):
        self.assertEqual(self.fullDate.day, "01")
        self.assertEqual(self.yearDate.day, None)
        self.assertEqual(self.monthDate.day, None)
