## vtfunc

Implement SQLite [table-valued functions](http://sqlite.org/vtab.html#tabfunc2) using Python.

### Rationale

SQLite makes it easy to define scalar and aggregate functions, but it is more challenging to create functions that return multiple values. Scalar functions accept zero or more parameters and return a single value. Aggregate functions accept parameters from any number of input rows, and then generate a final scalar value.

To create functions that return multiple values, it is necessary to create a [virtual table](http://sqlite.org/vtab.html). SQLite has the concept of "eponymous" virtual tables, which are virtual tables that can be called like a function and do not require explicit creation using DDL statements.

The `vtfunc` module abstracts away the complexity of creating an eponymous virtual table, allowing you to write your own multi-value SQLite functions in Python.

### Example

Suppose we want to create a function that, given a regular expression and an input string, returns all matching subgroups in the input string. For instance, if our regex was `'[0-9]+'` and our input string was `'123 xxx 456 yyy 789 zzz 0'`, the function should return four rows:

* `123`
* `456`
* `789`
* `0`

With the `vtab` module it is very easy to implement this:

```python
import re

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
```

To use our function, we need to register the module with a SQLite connection, then call it using a `SELECT` query:

```python

import sqlite3

conn = sqlite3.connect(':memory:')  # Create an in-memory database.

search_module = RegexSearch()
search_module.register(conn)  # Register our module.

query_params = ('[0-9]+', '123 xxx 456 yyy 789 zzz 0')
cursor = conn.execute('SELECT * FROM regex_search(?, ?);', query_params)
print cursor.fetchall()
```
