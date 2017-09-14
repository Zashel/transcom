import time
from zashel.gapi import GoogleAPI, SHEETS, Spreadsheets

NEXT_FUNCTION = """=getNext({b};{cols_numbers};{filters};{a};{initial})""" #B First, A Next

class SharedSpreadsheets(Spreadsheets):
    """Only to use with an specific format"""
    class Sheet(Spreadsheets.Sheet):
        class Row(Spreadsheets.Sheet.Row):
            def __getitem__(self, key):
                if (self.sheet_name == self.spreadsheet.datasheet.sheet_name and
                        isinstance(key, str) and key in self.headers):
                    key = self.headers.index(key)
                return Spreadsheets.Sheet.Row.__getitem__(self, key)

            def __setitem__(self, key, value):
                if (self.sheet_name == self.spreadsheet.datasheet.sheet_name and
                        isinstance(key, str) and key in self.headers):
                    key = self.headers.index(key)
                Spreadsheets.Sheet.Row.__setitem__(self, key, value)

            @property
            def headers(self):
                return self.spreadsheet.headers

        def row(self, key, range):
            return SharedSpreadsheets.Sheet.Row(key, range, self)

    def __init__(self, gapi, name, datasheet="Datos", blocksheet="__Block__"):
        Spreadsheets.__init__(self, gapi, name)
        self._datasheet = self[datasheet]
        self._headers = self.datasheet[0]
        self._blocksheet = self[blocksheet]
        self._my_row = None
        self._function = None
        for index, item in enumerate(self.blocksheet):
            #print(index, item, sep="\t")
            try:
                if item[0] == self.api.uuid:
                    self._my_row = index
                    self._function = NEXT_FUNCTION.format(b="B" + str(index + 1), a="A" + str(index + 1),
                                                          cols_numbers="{cols_numbers}",
                                                          filters="{filters}",
                                                          initial="{initial}")
                    break
            except IndexError:
                pass
        else:
            range = self._blocksheet.append_row("A1", [self.api.uuid, self.datasheet.sheet_name])
            init, fin = range.split(":")
            self._function = NEXT_FUNCTION.format(b=fin, a=init,
                                                  cols_numbers="{cols_numbers}",
                                                  filters="{filters}",
                                                  initial="{initial}")
            self._my_row = int(fin.strip("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        self._blocked_row = 1

    def __del__(self):
        self.blocksheet[self._my_row][2] = self.function.format(cols_numbers=[],
                                                                filters=[],
                                                                initial=-1)

    @property
    def datasheet(self):
        return self._datasheet

    @property
    def blocksheet(self):
        return self._blocksheet

    @property
    def function(self):
        return self._function

    @property
    def headers(self):
        return self._headers

    @property
    def my_row(self):
        return self._my_row

    def get_next(self, columns, filter):
        columns = [str(column) for column in columns]
        columns = "{"+";".join(columns)+"}"
        filter = ["\""+f+"\"" for f in filter]
        filter = "{" + ";".join(filter) + "}"
        self.blocksheet[self._my_row][2] = self.function.format(cols_numbers=columns,
                                                                filters=filter,
                                                                initial=self._blocked_row)
        while True:
            blocked = self.blocksheet[self._my_row][2]
            if blocked == "EOF":
                self.blocksheet[self._my_row][2] = self.function.format(cols_numbers=columns,
                                                                        filters=filter,
                                                                        initial=-1)
                self._blocked_row = 1
                raise EOFError()
            elif blocked in ("Loading...", "Cargando..."):
                time.sleep(2)
            elif blocked == "#ERROR!":
                self.blocksheet[self._my_row][2] = self.function.format(cols_numbers=columns,
                                                                        filters=filter,
                                                                        initial=self._blocked_row)
            else:
                self._blocked_row = int(blocked)
                return SharedSpreadsheets.Sheet.Row(self._blocked_row-1,
                                                    self.blocksheet[self._my_row][3:],
                                                    self.datasheet)

    def sheet(self, sheet):
        return SharedSpreadsheets.Sheet(sheet, self.api, self.name, self)


class TranscomAPI(GoogleAPI):
    def __init__(self, scopes, secret_file=None, secret_data=None, password=None):
        GoogleAPI.__init__(self, scopes=scopes, secret_file=secret_file, secret_data=secret_data, password=password)

    def spreadsheet_open_shared(self, name=None, **kwargs):
        if name is None and "name" in kwargs:
            name = kwargs["name"]
        elif name is None:
            raise FileNotFoundError()
        return self._files_open(SHEETS, SharedSpreadsheets, name, self.spreadsheets)

#TODO: More Friendly
