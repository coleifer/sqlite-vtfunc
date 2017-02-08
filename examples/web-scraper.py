#!/usr/bin/env python

import re
import urllib2

from pysqlite2 import dbapi2 as sqlite3
from vtfunc import TableFunction


class Scraper(TableFunction):
    params = ['url']
    columns = ['href', 'description']
    name = 'scraper'

    def initialize(self, url):
        self._iter = re.finditer(
            '<a[^\>]+?href="([^\"]+?)"[^\>]*?>([^\<]+?)</a>',
            urllib2.urlopen(url).read())

    def iterate(self, idx):
        return next(self._iter).groups()


conn = sqlite3.connect(':memory:')

# Register the module with the connection.
Scraper.register(conn)

# Scrape the HackerNews front-page and query for the 3 links with the longest
# descriptions.
cursor = conn.execute(
    'SELECT * FROM scraper(?) '
    'ORDER BY length(description) DESC LIMIT 3;',
    ('https://news.ycombinator.com',))
for href, desc in cursor.fetchall():
    print desc, ':', href


# Create a separate table that stores a list of URLs we need to scrape. Since
# this is a relational database, we can feed the url-list to our table-function
# quite easily.
conn.execute('CREATE TABLE url_list (url TEXT PRIMARY KEY);')
conn.execute('INSERT INTO url_list (url) VALUES (?), (?), (?);', (
    'http://docs.peewee-orm.com/en/latest/',
    'http://github.com/coleifer',
    'https://news.ycombinator.com'))

query = conn.execute('SELECT s.url, href, description FROM '
                     'url_list AS s, scraper(s.url)')
for url, href, description in query:
    print url, ' -- ', description, ':', href
