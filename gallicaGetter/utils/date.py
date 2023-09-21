import re


class Date:
    dateFormats = [
        re.compile(r"^\d{4}-\d{1,2}-\d{1,2}$"),
        re.compile(r"^\d{4}-\d{1,2}$"),
        re.compile(r"^\d{4}$"),
    ]

    def __init__(self, dateText):
        dateText = str(dateText)
        self.date = ["", "", ""]
        self.dateText = dateText
        for dateFormat in Date.dateFormats:
            if dateFormat.match(dateText):
                for index, entry in enumerate(dateText.split("-")):
                    self.date[index] = entry

    def __repr__(self):
        return self.dateText

    @property
    def year(self) -> str:
        return self.date[0]

    @property
    def month(self) -> str:
        return self.date[1]

    @property
    def day(self) -> str:
        return self.date[2]
