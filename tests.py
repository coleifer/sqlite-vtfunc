import re
import sys
import unittest

import sqlite3
from vtfunc import TableFunction


class Series(TableFunction):
    columns = ['value']
    params = ['start', 'stop', 'step']
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


class RegexSearch(TableFunction):
    columns = ['match']
    params = ['regex', 'search_string']
    name = 'regex_search'

    def initialize(self, regex=None, search_string=None):
        if regex and search_string:
            self._iter = re.finditer(regex, search_string)
        else:
            self._iter = None

    def iterate(self, idx):
        # We do not need `idx`, so just ignore it.
        if self._iter is None:
            raise StopIteration
        else:
            return (next(self._iter).group(0),)


class Split(TableFunction):
    params = ['data']
    columns = ['part']
    name = 'str_split'

    def initialize(self, data=None):
        self._parts = data.split()
        self._idx = 0

    def iterate(self, idx):
        if self._idx < len(self._parts):
            result = (self._parts[self._idx],)
            self._idx += 1
            return result
        raise StopIteration


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')

    def tearDown(self):
        self.conn.close()

    def execute(self, sql, params=None):
        return self.conn.execute(sql, params or ())


class TestErrorHandling(BaseTestCase):
    def test_error_instantiate(self):
        class BrokenInstantiate(Series):
            name = 'broken_instantiate'

            def __init__(self, *args, **kwargs):
                super(BrokenInstantiate, self).__init__(*args, **kwargs)
                raise ValueError('broken instantiate')

        BrokenInstantiate.register(self.conn)
        self.assertRaises(sqlite3.OperationalError, self.execute,
                          'SELECT * FROM broken_instantiate(1, 10)')

    def test_error_init(self):
        class BrokenInit(Series):
            name = 'broken_init'

            def initialize(self, start=0, stop=None, step=1):
                raise ValueError('broken init')

        BrokenInit.register(self.conn)
        self.assertRaises(sqlite3.OperationalError, self.execute,
                          'SELECT * FROM broken_init(1, 10)')
        self.assertRaises(sqlite3.OperationalError, self.execute,
                          'SELECT * FROM broken_init(0, 1)')

    def test_error_iterate(self):
        class BrokenIterate(Series):
            name = 'broken_iterate'

            def iterate(self, idx):
                raise ValueError('broken iterate')

        BrokenIterate.register(self.conn)
        self.assertRaises(sqlite3.OperationalError, self.execute,
                          'SELECT * FROM broken_iterate(1, 10)')
        self.assertRaises(sqlite3.OperationalError, self.execute,
                          'SELECT * FROM broken_iterate(0, 1)')

    def test_error_iterate_delayed(self):
        # Only raises an exception if the value 7 comes up.
        class SomewhatBroken(Series):
            name = 'somewhat_broken'

            def iterate(self, idx):
                ret = super(SomewhatBroken, self).iterate(idx)
                if ret == (7,):
                    raise ValueError('somewhat broken')
                else:
                    return ret

        SomewhatBroken.register(self.conn)
        curs = self.execute('SELECT * FROM somewhat_broken(0, 3)')
        self.assertEqual(curs.fetchall(), [(0,), (1,), (2,), (3,)])

        curs = self.execute('SELECT * FROM somewhat_broken(5, 8)')
        self.assertEqual(curs.fetchone(), (5,))
        self.assertRaises(sqlite3.OperationalError, curs.fetchall)

        curs = self.execute('SELECT * FROM somewhat_broken(0, 2)')
        self.assertEqual(curs.fetchall(), [(0,), (1,), (2,)])


class TestTableFunction(BaseTestCase):
    def test_split(self):
        Split.register(self.conn)
        curs = self.execute('select part from str_split(?) order by part '
                            'limit 3', ('well hello huey and zaizee',))
        self.assertEqual([row for row, in curs.fetchall()],
                         ['and', 'hello', 'huey'])

    def test_split_tbl(self):
        Split.register(self.conn)
        self.execute('create table post (content TEXT);')
        self.execute('insert into post (content) values (?), (?), (?)',
                     ('huey secret post',
                      'mickey message',
                      'zaizee diary'))
        curs = self.execute('SELECT * FROM post, str_split(post.content)')
        results = curs.fetchall()
        self.assertEqual(results, [
            ('huey secret post', 'huey'),
            ('huey secret post', 'secret'),
            ('huey secret post', 'post'),
            ('mickey message', 'mickey'),
            ('mickey message', 'message'),
            ('zaizee diary', 'zaizee'),
            ('zaizee diary', 'diary'),
        ])

    def test_series(self):
        Series.register(self.conn)

        def assertSeries(params, values, extra_sql=''):
            param_sql = ', '.join('?' * len(params))
            sql = 'SELECT * FROM series(%s)' % param_sql
            if extra_sql:
                sql = ' '.join((sql, extra_sql))
            curs = self.execute(sql, params)
            self.assertEqual([row for row, in curs.fetchall()], values)

        assertSeries((0, 10, 2), [0, 2, 4, 6, 8, 10])
        assertSeries((5, None, 20), [5, 25, 45, 65, 85], 'LIMIT 5')
        assertSeries((4, 0, -1), [4, 3, 2], 'LIMIT 3')
        assertSeries((3, 5, 3), [3])
        assertSeries((3, 3, 1), [3])

    def test_series_tbl(self):
        Series.register(self.conn)
        self.execute('CREATE TABLE nums (id INTEGER PRIMARY KEY)')
        self.execute('INSERT INTO nums DEFAULT VALUES;')
        self.execute('INSERT INTO nums DEFAULT VALUES;')
        curs = self.execute('SELECT * FROM nums, series(nums.id, nums.id + 2)')
        results = curs.fetchall()
        self.assertEqual(results, [
            (1, 1), (1, 2), (1, 3),
            (2, 2), (2, 3), (2, 4)])

        curs = self.execute('SELECT * FROM nums, series(nums.id) LIMIT 3')
        results = curs.fetchall()
        self.assertEqual(results, [(1, 1), (1, 2), (1, 3)])

    def test_regex(self):
        RegexSearch.register(self.conn)

        def assertResults(regex, search_string, values):
            sql = 'SELECT * FROM regex_search(?, ?)'
            curs = self.execute(sql, (regex, search_string))
            self.assertEqual([row for row, in curs.fetchall()], values)

        assertResults(
            '[0-9]+',
            'foo 123 45 bar 678 nuggie 9.0',
            ['123', '45', '678', '9', '0'])
        assertResults(
            '[\w]+@[\w]+\.[\w]{2,3}',
            ('Dear charlie@example.com, this is nug@baz.com. I am writing on '
             'behalf of zaizee@foo.io. He dislikes your blog.'),
            ['charlie@example.com', 'nug@baz.com', 'zaizee@foo.io'])
        assertResults(
            '[a-z]+',
            '123.pDDFeewXee',
            ['p', 'eew', 'ee'])
        assertResults(
            '[0-9]+',
            'hello',
            [])

    def test_regex_tbl(self):
        messages = (
            'hello foo@example.fap, this is nuggie@example.fap. How are you?',
            'baz@example.com wishes to let charlie@crappyblog.com know that '
            'huey@example.com hates his blog',
            'testing no emails.',
            '')
        RegexSearch.register(self.conn)

        self.execute('create table posts (id integer primary key, msg)')
        self.execute('insert into posts (msg) values (?), (?), (?), (?)',
                     messages)
        cur = self.execute('select posts.id, regex_search.rowid, regex_search.match '
                           'FROM posts, regex_search(?, posts.msg)',
                           ('[\w]+@[\w]+\.\w{2,3}',))
        results = cur.fetchall()
        self.assertEqual(results, [
            (1, 1, 'foo@example.fap'),
            (1, 2, 'nuggie@example.fap'),
            (2, 3, 'baz@example.com'),
            (2, 4, 'charlie@crappyblog.com'),
            (2, 5, 'huey@example.com'),
        ])


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
