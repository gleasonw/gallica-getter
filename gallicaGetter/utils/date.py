import re
import weakref


class Date:
    """Caches dates for memory efficiency."""

    _cache = {}
    dateFormats = [
        re.compile(r"^\d{4}-\d{1,2}-\d{1,2}$"),
        re.compile(r"^\d{4}-\d{1,2}$"),
        re.compile(r"^\d{4}$"),
    ]

    def __str__(self):
        return self.dateText

    @staticmethod
    def __new__(cls, dateText):
        dateText = str(dateText)
        selfref = Date._cache.get(dateText)
        if not selfref:
            self = super().__new__(cls)
            self.dateText = dateText
            self.date = Date.parseDateText(dateText)
            Date._cache[dateText] = weakref.ref(self)
        else:
            self = selfref()
        return self

    @staticmethod
    def parseDateText(dateText):
        date = [None, None, None]
        for dateFormat in Date.dateFormats:
            if dateFormat.match(dateText):
                for index, entry in enumerate(dateText.split("-")):
                    date[index] = entry
                return date
        return date

    def __init__(self, dateText):
        pass

    def __repr__(self):
        return f"Date({self.date})"

    @property
    def year(self) -> str:
        return self.date[0]

    @property
    def month(self) -> str:
        return self.date[1]

    @property
    def day(self) -> str:
        return self.date[2]

    def __del__(self):
        del Date._cache[self.dateText]
