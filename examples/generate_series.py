import sqlite3
from vtfunc import TableFunction


class GenerateSeries(TableFunction):
    params = ['start', 'stop', 'step']
    columns = ['output']
    name = 'series'

    def initialize(self, start=0, stop=None, step=1):
        self.start = start
        self.stop = stop or float('inf')
        self.step = step
        self.curr = self.start

    def iterate(self, idx):
        if self.curr > self.stop:
            raise StopIteration

        ret = self.curr
        self.curr += self.step
        return (ret,)


conn = sqlite3.connect(':memory:')

GenerateSeries.register(conn)

cursor = conn.execute('SELECT * FROM series(0, 10, 2)')
print(cursor.fetchall())

cursor = conn.execute('SELECT * FROM series(5, NULL, 20) LIMIT 10')
print(cursor.fetchall())
