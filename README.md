## vtfunc

**NOTICE**: This project is no longer necessary if you are using Peewee 3.0 or
newer, as the relevant code has been included in Peewee's sqlite extension
module. For more information, see:

* [Peewee user-defined function examples](http://docs.peewee-orm.com/en/latest/peewee/database.html#user-defined-functions)
* [TableFunction API documentation](http://docs.peewee-orm.com/en/latest/peewee/sqlite_ext.html#TableFunction)
* [Table function registration API](http://docs.peewee-orm.com/en/latest/peewee/api.html#SqliteDatabase.table_function)
* [General SQLite extensions documentation](http://docs.peewee-orm.com/en/latest/peewee/sqlite_ext.html)

If you intend to use this project with an older version of Peewee, or as a
standalone project with the standard library SQLite module, feel free to
continue using this repository.

--------------------------------------------------------------------------

Python bindings for the creation of [table-valued functions](http://sqlite.org/vtab.html#tabfunc2)
in SQLite.

A table-valued function:

* Accepts any number of parameters
* Can be used in places you would put a normal table or subquery, such as the
  `FROM` clause or on the right-hand-side of an `IN` expression.
* may return an arbitrary number of rows consisting of one or more columns.

Here are some examples of what you can do with Python and `sqlite-vtfunc`:

* Write a SELECT query that, when run, will scrape a website and return a table
  of all the outbound links on the page (rows are `(href, description)`
  tuples. [See example below](#scraping-pages-with-sql)).
* Accept a file path and return a table of the files in that directory and
  their associated metadata.
* Use table-valued functions to handle recurring events in a calendaring
  application (by generating the series of recurrances dynamically).
* Apply a regular expression search to some text and return a row for each
  matching substring.

### Scraping pages with SQL

To get an idea of how `sqlite-vtfunc` works, let's build the scraper table
function described in the previous section. The function will accept a URL as
the only parameter, and will return a table of the link destinations and text
descriptions.

The `Scraper` class contains the entire implementation for the scraper:

```python

import re, urllib2

from pysqlite2 import dbapi2 as sqlite3  # Use forked pysqlite.
from vtfunc import TableFunction


class Scraper(TableFunction):
    params = ['url']  # Function argument names.
    columns = ['href', 'description']  # Result rows have these columns.
    name = 'scraper'  # Name we use to invoke the function from SQL.

    def initialize(self, url):
        # When the function is called, download the HTML and create an
        # iterator that successively yields `href`/`description` pairs.
        fh = urllib2.urlopen(url)
        self.html = fh.read()
        self._iter = re.finditer(
            '<a[^\>]+?href="([^\"]+?)"[^\>]*?>([^\<]+?)</a>',
            self.html)

    def iterate(self, idx):
        # Since row ids would not be meaningful for this particular table-
        # function, we can ignore "idx" and just advance the regex iterator.

        # Ordinarily, to signal that there are no more rows, the `iterate()`
        # method must raise a `StopIteration` exception. This is not necessary
        # here because `next()` will raise the exception when the regex
        # iterator is finished.
        return next(self._iter).groups()
```

To start using the table function, create a connection and register the table
function with the connection. **Note**: for SQLite version <= 3.13, the table
function will not remain loaded across connections, so it is necessary to
register it each time you connect to the database.

```python

# Creating a connection and registering our scraper function.
conn = sqlite3.connect(':memory:')
Scraper.register(conn)  # Register the function with the new connection.
```

To test the scraper, start up a python interpreter and enter the above code.
Once that is done, let's try a query. The following query will fetch the HTML
for the hackernews front-page and extract the three links with the longest
descriptions:

```pycon
>>> curs = conn.execute('SELECT * FROM scraper(?) '
...                     'ORDER BY length(description) DESC '
...                     'LIMIT 3', ('https://news.ycombinator.com/',))

>>> for (href, description) in curs.fetchall():
...     print description, ':', href

The Diolkos: an ancient Greek paved trackway enabling boats to be moved overland : https://...
The NumPy array: a structure for efficient numerical computation (2011) [pdf] : https://hal...
Restoring Y Combinator's Xerox Alto, day 4: What's running on the system : http://www.right...
```

Now, suppose you have another table which contains a huge list of URLs that you
need to scrape. Since this is a relational database, it's incredibly easy to
connect the URLs in one table with another.

The following query will scrape all the URLs in the `unvisited_urls` table:

```sql

SELECT uu.url, href, description
FROM unvisited_urls AS uu, scraper(uu.url)
ORDER BY uu.url, href, description;
```

### Example two: implementing Python's range()

This function generates a series of integers between given boundaries and at
given intervals.

```python

from vtfunc import TableFunction


class GenerateSeries(TableFunction):
    params = ['start', 'stop', 'step']
    columns = ['output']
    name = 'generate_series'

    def initialize(self, start=0, stop=None, step=1):
        # Note that when a parameter is optional, the only thing
        # you need to do is provide a default value in `initialize()`.
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
```

### Dependencies

This project is designed to work with the standard library `sqlite3` driver, or
alternatively, the latest version of `pysqlite2`.

### Implementation Notes

To create functions that return multiple values, it is necessary to create a
[virtual table](http://sqlite.org/vtab.html). SQLite has the concept of
"eponymous" virtual tables, which are virtual tables that can be called like a
function and do not require explicit creation using DDL statements.

The `vtfunc` module abstracts away the complexity of creating an eponymous
virtual table, allowing you to write your own multi-value SQLite functions in
Python.

# TODO: was removing stuff and stopped here.

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

To use our function, we need to register the module with a SQLite connection,
then call it using a `SELECT` query:

```python

import sqlite3

conn = sqlite3.connect(':memory:')  # Create an in-memory database.

RegexSearch.register(conn)  # Register our module.

query_params = ('[0-9]+', '123 xxx 456 yyy 789 zzz 0')
cursor = conn.execute('SELECT * FROM regex_search(?, ?);', query_params)
print cursor.fetchall()
```

Let's say we have a table that contains a list of arbitrary messages and we
want to capture all the e-mail addresses from that table. This is also easy
using our table-valued function. We will query the `messages` table and pass
the message body into our table-valued function. Then, for each email address
we find, we'll return a row containing the message ID and the matching email
address:

```python

email_regex = '[\w]+@[\w]+\.[\w]{2,3}'  # Stupid simple email regex.
query = ('SELECT messages.id, regex_search.match '
         'FROM messages, regex_search(?, messages.body)')
cursor = conn.execute(query, (email_regex,))
```

The resulting rows will look something like:

```

message id |         email
-----------+-----------------------
     1     | charlie@example.com
     1     | huey@kitty.cat
     1     | zaizee@morekitties.cat
     3     | mickey@puppies.dog
     3     | huey@throwaway.cat
    ...    |         ...
```

#### Important note

In the above example you will note that the parameters for our query actually
change (because each row in the messages table has a different search string).
This means that for this particular query, the `RegexSearch.initialize()`
function will be called once for each row in the `messages` table.

### How it works

Behind-the-scenes, `vtfunc` is creating a [Virtual Table](http://sqlite.org/vtab.html)
and filling in the various callbacks with wrappers around your user-defined
function. There are two important methods that the wrapped virtual table
implements:

* xBestIndex
* xFilter

When SQLite attempts to execute a query, it will call the xBestIndex method of
the virtual table (possibly multiple times) trying to come up with the best
query plan. The `vtfunc` module optimizes for those query plans which include
values for all the parameters of the user-defined function. Since some
user-defined functions may have optional parameters, query plans with only a
subset of param values will be slightly penalized.

Since we have no visibility into what parameters the user *actually* passed in,
and we don't know ahead of time which query plan SQLite suggests will be
best, `vtfunc` just does its best to optimize for plans with the highest
number of usable parameter values.

If you encounter a situation where you pass your function multiple parameters,
but it doesn't receive all of them, it's the case that a less-than-optimal
plan was used.

After the plan is chosen by calling xBestIndex, the query will execute by
calling xFilter (possibly multiple times). xFilter has access to the actual
query parameters, and it's responsibility is to initialize the cursor and call
the user's initialize() callback with the parameters passed in.
