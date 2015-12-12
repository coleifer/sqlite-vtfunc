import re
import sqlite3

from vtfunc import TableFunction


class RegexSearch(TableFunction):
    params = ['regex', 'search_string']
    columns = ['match']
    name = 'regex_search'

    def initialize(self, regex=None, search_string=None):
        self._iter = re.finditer(regex, search_string)

    def iterate(self, idx):
        # We do not need `idx`, so just ignore it.
        return (next(self._iter).group(0),)


conn = sqlite3.connect(':memory:')

# Register the module with the connection.
RegexSearch.register(conn)

# Query the module.
query_params = ('[0-9]+', '1337 foo 567 bar 999 baz 0123')
cursor = conn.execute('SELECT * FROM regex_search(?, ?);', query_params)
print cursor.fetchall()
