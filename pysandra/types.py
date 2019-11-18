class Types:
    pass


class UnknownType(Types):
    pass


class AsciiType(Types):
    pass


class Rows:
    def __init__(self, column_count=None):
        self.index = 0
        self._data = []
        self.column_count = column_count

    def __iter__(self):
        return self

    def add(self, cell):
        self._data.append(cell)

    def __next__(self):
        if self.index == len(self._data):
            # reset
            self.index = 0
            raise StopIteration
        current = self._data[self.index : self.index + self.column_count]
        self.index += self.column_count
        return current


if __name__ == "__main__":
    d = Rows(column_count=2)
    d.add("1")
    d.add("2")
    d.add("3")
    d.add("4")
    for row in d:
        print(f"got row={row}")
    for row in d:
        print(f"got row={row}")
