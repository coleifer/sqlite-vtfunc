from cpython.object cimport PyObject
from cpython.ref cimport Py_INCREF, Py_DECREF
from libc.stdlib cimport free, malloc
from libc.string cimport memcpy
from libc.string cimport memset


cdef struct sqlite3_index_constraint:
    int iColumn
    unsigned char op
    unsigned char usable
    int iTermOffset


cdef struct sqlite3_index_orderby:
    int iColumn
    unsigned char desc


cdef struct sqlite3_index_constraint_usage:
    int argvIndex
    unsigned char omit


cdef extern from "sqlite3.h":
    ctypedef struct sqlite3
    ctypedef struct sqlite3_context
    ctypedef struct sqlite3_value
    ctypedef long long sqlite3_int64

    # Virtual tables.
    ctypedef struct sqlite3_module  # Forward reference.
    ctypedef struct sqlite3_vtab:
        const sqlite3_module *pModule
        int nRef
        char *zErrMsg
    ctypedef struct sqlite3_vtab_cursor:
        sqlite3_vtab *pVtab

    ctypedef struct sqlite3_index_info:
        int nConstraint
        sqlite3_index_constraint *aConstraint
        int nOrderBy
        sqlite3_index_orderby *aOrderBy
        sqlite3_index_constraint_usage *aConstraintUsage
        int idxNum
        char *idxStr
        int needToFreeIdxStr
        int orderByConsumed
        double estimatedCost
        sqlite3_int64 estimatedRows
        int idxFlags

    ctypedef struct sqlite3_module:
        int iVersion
        int (*xCreate)(sqlite3*, void *pAux, int argc, char **argv,
                       sqlite3_vtab **ppVTab, char**)
        int (*xConnect)(sqlite3*, void *pAux, int argc, char **argv,
                        sqlite3_vtab **ppVTab, char**)
        int (*xBestIndex)(sqlite3_vtab *pVTab, sqlite3_index_info*)
        int (*xDisconnect)(sqlite3_vtab *pVTab)
        int (*xDestroy)(sqlite3_vtab *pVTab)
        int (*xOpen)(sqlite3_vtab *pVTab, sqlite3_vtab_cursor **ppCursor)
        int (*xClose)(sqlite3_vtab_cursor*)
        int (*xFilter)(sqlite3_vtab_cursor*, int idxNum, const char *idxStr,
                       int argc, sqlite3_value **argv)
        int (*xNext)(sqlite3_vtab_cursor*)
        int (*xEof)(sqlite3_vtab_cursor*)
        int (*xColumn)(sqlite3_vtab_cursor*, sqlite3_context *, int)
        int (*xRowid)(sqlite3_vtab_cursor*, sqlite3_int64 *pRowid)
        int (*xUpdate)(sqlite3_vtab *pVTab, int, sqlite3_value **,
                       sqlite3_int64 **)
        int (*xBegin)(sqlite3_vtab *pVTab)
        int (*xSync)(sqlite3_vtab *pVTab)
        int (*xCommit)(sqlite3_vtab *pVTab)
        int (*xRollback)(sqlite3_vtab *pVTab)
        int (*xFindFunction)(sqlite3_vtab *pVTab, int nArg, const char *zName,
                             void (**pxFunc)(sqlite3_context *, int,
                                             sqlite3_value **),
                             void **ppArg)
        int (*xRename)(sqlite3_vtab *pVTab, const char *zNew)
        int (*xSavepoint)(sqlite3_vtab *pVTab, int)
        int (*xRelease)(sqlite3_vtab *pVTab, int)
        int (*xRollbackTo)(sqlite3_vtab *pVTab, int)

    cdef int sqlite3_declare_vtab(sqlite3 *db, const char *zSQL)
    cdef int sqlite3_create_module(sqlite3 *db, const char *zName,
                                   const sqlite3_module *p, void *pClientData)

    # Encoding.
    cdef int SQLITE_UTF8 = 1

    # Return values.
    cdef int SQLITE_OK = 0
    cdef int SQLITE_ERROR = 1
    cdef int SQLITE_NOMEM = 7

    # Types of filtering operations.
    cdef int SQLITE_INDEX_CONSTRAINT_EQ = 2
    cdef int SQLITE_INDEX_CONSTRAINT_GT = 4
    cdef int SQLITE_INDEX_CONSTRAINT_LE = 8
    cdef int SQLITE_INDEX_CONSTRAINT_LT = 16
    cdef int SQLITE_INDEX_CONSTRAINT_GE = 32
    cdef int SQLITE_INDEX_CONSTRAINT_MATCH = 64

    # sqlite_value_type.
    cdef int SQLITE_INTEGER = 1
    cdef int SQLITE_FLOAT   = 2
    cdef int SQLITE3_TEXT   = 3
    cdef int SQLITE_TEXT    = 3
    cdef int SQLITE_BLOB    = 4
    cdef int SQLITE_NULL    = 5

    ctypedef void (*sqlite3_destructor_type)(void*)

    cdef int sqlite3_create_function(
        sqlite3 *db,
        const char *zFunctionName,
        int nArg,
        int eTextRep,  # SQLITE_UTF8
        void *pApp,  # App-specific data.
        void (*xFunc)(sqlite3_context *, int, sqlite3_value **),
        void (*xStep)(sqlite3_context*, int, sqlite3_value **),
        void (*xFinal)(sqlite3_context*),
    )

    # Converting from Sqlite -> Python.
    cdef const void *sqlite3_value_blob(sqlite3_value*);
    cdef int sqlite3_value_bytes(sqlite3_value*);
    cdef double sqlite3_value_double(sqlite3_value*);
    cdef int sqlite3_value_int(sqlite3_value*);
    cdef sqlite3_int64 sqlite3_value_int64(sqlite3_value*);
    cdef const unsigned char *sqlite3_value_text(sqlite3_value*);
    cdef int sqlite3_value_type(sqlite3_value*);
    cdef int sqlite3_value_numeric_type(sqlite3_value*);

    # Converting from Python -> Sqlite.
    cdef void sqlite3_result_double(sqlite3_context*, double)
    cdef void sqlite3_result_error(sqlite3_context*, const char*, int)
    cdef void sqlite3_result_error_toobig(sqlite3_context*)
    cdef void sqlite3_result_error_nomem(sqlite3_context*)
    cdef void sqlite3_result_error_code(sqlite3_context*, int)
    cdef void sqlite3_result_int(sqlite3_context*, int)
    cdef void sqlite3_result_int64(sqlite3_context*, sqlite3_int64)
    cdef void sqlite3_result_null(sqlite3_context*)
    cdef void sqlite3_result_text(sqlite3_context*, const char*, int, void(*)(void*))
    cdef void sqlite3_result_value(sqlite3_context*, sqlite3_value*)

    # Memory management.
    cdef void* sqlite3_malloc(int)
    cdef void sqlite3_free(void *)


cdef extern from "pysqlite/connection.h":
    # Extract the underlying database handle from a Python connection object.
    ctypedef struct pysqlite_Connection:
        sqlite3 *db


ctypedef struct peewee_vtab:
    sqlite3_vtab base
    void *table_func


ctypedef struct peewee_cursor:
    sqlite3_vtab_cursor base
    long long idx
    void *table_func
    void *row_data
    bint stopped


cdef int pwConnect(sqlite3 *db, void *pAux, int argc, char **argv,
                   sqlite3_vtab **ppVtab, char **pzErr) with gil:
    cdef:
        int rc
        peewee_vtab *pNew
        _TableFunction table_func = <_TableFunction>pAux

    rc = sqlite3_declare_vtab(
        db,
        'CREATE TABLE x(%s);' % table_func.get_table_columns_declaration())
    if rc == SQLITE_OK:
        pNew = <peewee_vtab *>sqlite3_malloc(sizeof(pNew[0]))
        memset(<char *>pNew, 0, sizeof(pNew[0]))
        ppVtab[0] = &(pNew.base)

        pNew.table_func = <void *>table_func
        Py_INCREF(table_func)

    return rc


cdef int pwDisconnect(sqlite3_vtab *pBase) with gil:
    cdef:
        peewee_vtab *pVtab = <peewee_vtab *>pBase
        _TableFunction table_func = <_TableFunction>pVtab.table_func

    Py_DECREF(table_func)
    sqlite3_free(pVtab)
    return SQLITE_OK


cdef int pwOpen(sqlite3_vtab *pBase, sqlite3_vtab_cursor **ppCursor):
    cdef:
        peewee_vtab *pVtab = <peewee_vtab *>pBase
        peewee_cursor *pCur

    pCur = <peewee_cursor *>sqlite3_malloc(sizeof(pCur[0]))
    memset(<char *>pCur, 0, sizeof(pCur[0]))
    ppCursor[0] = &(pCur.base)
    pCur.idx = 0
    pCur.table_func = pVtab.table_func
    pCur.stopped = False
    return SQLITE_OK


cdef int pwClose(sqlite3_vtab_cursor *pBase):
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
    sqlite3_free(pCur)
    return SQLITE_OK


cdef int pwNext(sqlite3_vtab_cursor *pBase) with gil:
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
        _TableFunction table_func = <_TableFunction>pCur.table_func
        tuple result

    if pCur.row_data:
        Py_DECREF(<tuple>pCur.row_data)

    try:
        result = table_func.next_func(pCur.idx)
    except StopIteration:
        pCur.stopped = True
    except:
        return SQLITE_ERROR
    else:
        Py_INCREF(result)
        pCur.row_data = <void *>result
        pCur.idx += 1
        pCur.stopped = False

    return SQLITE_OK


cdef int pwColumn(sqlite3_vtab_cursor *pBase, sqlite3_context *ctx,
                  int iCol) with gil:
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
        sqlite3_int64 x = 0
        tuple row_data

    if iCol == -1:
        sqlite3_result_int64(ctx, <sqlite3_int64>pCur.idx)
        return SQLITE_OK

    row_data = <tuple>pCur.row_data
    value = row_data[iCol]
    if value is None:
        sqlite3_result_null(ctx)
    elif isinstance(value, (int, long)):
        sqlite3_result_int64(ctx, <sqlite3_int64>value)
    elif isinstance(value, float):
        sqlite3_result_double(ctx, <double>value)
    elif isinstance(value, basestring):
        sqlite3_result_text(
            ctx,
            <const char *>value,
            -1,
            <sqlite3_destructor_type>-1)
    elif isinstance(value, bool):
        sqlite3_result_int(ctx, int(value))
    else:
        sqlite3_result_error(ctx, 'Unsupported type %s' % type(value), -1)
        return SQLITE_ERROR

    return SQLITE_OK


cdef int pwRowid(sqlite3_vtab_cursor *pBase, sqlite3_int64 *pRowid) with gil:
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
    pRowid[0] = <sqlite3_int64>pCur.idx
    return SQLITE_OK


cdef int pwEof(sqlite3_vtab_cursor *pBase) with gil:
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
    if pCur.stopped:
        return 1
    return 0


cdef int pwFilter(sqlite3_vtab_cursor *pBase, int idxNum,
                  const char *idxStr, int argc, sqlite3_value **argv) with gil:
    cdef:
        peewee_cursor *pCur = <peewee_cursor *>pBase
        _TableFunction table_func = <_TableFunction>pCur.table_func
        dict query = {}
        int idx
        int value_type
        tuple row_data
        void *row_data_raw

    if not idxStr:
        return SQLITE_OK
    else:
        params = str(idxStr).split(',')

    for idx, param in enumerate(params):
        value = argv[idx]
        if not value:
            query[param] = None
            continue

        value_type = sqlite3_value_type(value)
        if value_type == SQLITE_INTEGER:
            query[param] = sqlite3_value_int(value)
        elif value_type == SQLITE_FLOAT:
            query[param] = sqlite3_value_double(value)
        elif value_type == SQLITE_TEXT:
            query[param] = str(sqlite3_value_text(value))
        elif value_type == SQLITE_BLOB:
            query[param] = <bytes>sqlite3_value_blob(value)
        elif value_type == SQLITE_NULL:
            query[param] = None
        else:
            query[param] = None

    table_func.init_func(**query)
    pCur.stopped = False
    try:
        row_data = table_func.next_func(0)
    except StopIteration:
        pCur.stopped = True
    else:
        Py_INCREF(row_data)
        pCur.row_data = <void *>row_data
        pCur.idx += 1
    return SQLITE_OK


cdef int pwBestIndex(sqlite3_vtab *pBase, sqlite3_index_info *pIdxInfo) \
        with gil:
    cdef:
        int i
        int idxNum = 0, nArg = 0
        peewee_vtab *pVtab = <peewee_vtab *>pBase
        _TableFunction table_func = <_TableFunction>pVtab.table_func
        sqlite3_index_constraint *pConstraint
        list columns = []
        char *idxStr

    pConstraint = <sqlite3_index_constraint*>0
    for i in range(pIdxInfo.nConstraint):
        pConstraint = &pIdxInfo.aConstraint[i]
        if not pConstraint.usable:
            continue
        if pConstraint.op != SQLITE_INDEX_CONSTRAINT_EQ:
            continue

        columns.append(table_func.param_names[pConstraint.iColumn - 1])
        nArg += 1
        pIdxInfo.aConstraintUsage[i].argvIndex = nArg
        pIdxInfo.aConstraintUsage[i].omit = 1

    # Both start and stop are specified. This is preferable.
    if columns:
        pIdxInfo.estimatedCost = <double>1
        pIdxInfo.estimatedRows = 1000
        joinedCols = ','.join(columns)
        idxStr = <char *>sqlite3_malloc((len(joinedCols) + 1) * sizeof(char))
        memcpy(idxStr, <char *>joinedCols, len(joinedCols))
        idxStr[len(joinedCols)] = '\x00'
        pIdxInfo.idxStr = idxStr
        pIdxInfo.needToFreeIdxStr = 1
    else:
        pIdxInfo.estimatedCost = <double>2000000
        pIdxInfo.estimatedRows = 1000
    return SQLITE_OK


cdef class _TableFunction(object):
    cdef:
        object init_func
        object next_func
        list param_names
        list column_names
        int row_length
        str name
        sqlite3_module module

    def __cinit__(self, init_func, next_func, param_names, column_names=None,
                  row_length=None, name=None):
        self.init_func = init_func
        self.next_func = next_func
        self.param_names = param_names
        self.column_names = column_names or ['result']
        self.row_length = row_length or len(self.column_names)
        self.name = name or type(self).__name__.lower()

        # Populate the SQLite module struct members.
        self.module.iVersion = 0
        self.module.xCreate = NULL
        self.module.xConnect = pwConnect
        self.module.xBestIndex = pwBestIndex
        self.module.xDisconnect = pwDisconnect
        self.module.xDestroy = NULL
        self.module.xOpen = pwOpen
        self.module.xClose = pwClose
        self.module.xFilter = pwFilter
        self.module.xNext = pwNext
        self.module.xEof = pwEof
        self.module.xColumn = pwColumn
        self.module.xRowid = pwRowid
        self.module.xUpdate = NULL
        self.module.xBegin = NULL
        self.module.xSync = NULL
        self.module.xCommit = NULL
        self.module.xRollback = NULL
        self.module.xFindFunction = NULL
        self.module.xRename = NULL

    cdef char* get_table_columns_declaration(self):
        cdef list accum = []

        for column in self.column_names:
            if isinstance(column, tuple):
                if len(column) != 2:
                    raise ValueError('Column must be either a string or a '
                                     '2-tuple of name, type')
                accum.append('%s %s' % column)
            else:
                accum.append(column)

        for param in self.param_names:
            accum.append('%s HIDDEN' % param)

        return ', '.join(accum)

    cpdef bint create_module(self, sqlite_conn):
        cdef:
            pysqlite_Connection *conn = <pysqlite_Connection *>sqlite_conn
            sqlite3 *db = conn.db
            int rc

        rc = sqlite3_create_module(
            db,
            <const char *>self.name,
            &self.module,
            <void *>self)
        return rc == SQLITE_OK


class TableFunction(object):
    """
    Implements a table-valued function (eponymous-only virtual table) in
    SQLite.

    Subclasses must define the columns (return values) and params (input
    values) to their function. These are defined as class attributes.

    The subclass also must implement two functions:

    * initialize(**query)
    * iterate(idx)

    The `initialize` function accepts the query parameters passed in from
    the SQL query. The `iterate` function accepts the index of the current
    iteration (zero-based) and must return a tuple of result values or raise
    a `StopIteration` to signal no more results.
    """
    columns = None
    params = None
    name = None

    def __init__(self):
        self._table_func = _TableFunction(
            self.initialize,
            self.iterate,
            self.params,
            self.columns,
            row_length=len(self.columns),
            name=self.name or type(self).__name__)

    def register(self, conn):
        self._table_func.create_module(conn)

    def initialize(self, **filters):
        raise NotImplementedError

    def iterate(self, idx):
        raise NotImplementedError
