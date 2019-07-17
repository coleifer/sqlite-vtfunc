# cython: language_level=3
from cpython.bytes cimport PyBytes_AsStringAndSize
from cpython.bytes cimport PyBytes_Check
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.object cimport PyObject
from cpython.tuple cimport PyTuple_New
from cpython.tuple cimport PyTuple_SET_ITEM
from cpython.unicode cimport PyUnicode_AsUTF8String
from cpython.unicode cimport PyUnicode_Check
from cpython.unicode cimport PyUnicode_DecodeUTF8
from cpython.ref cimport Py_INCREF, Py_DECREF
from libc.float cimport DBL_MAX
from libc.stdlib cimport rand
from libc.string cimport memcpy
from libc.string cimport memset

import traceback


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
    ctypedef struct sqlite3:
        int busyTimeout
    ctypedef struct sqlite3_context
    ctypedef struct sqlite3_value
    ctypedef long long sqlite3_int64
    ctypedef unsigned long long sqlite3_uint64

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
    cdef int SQLITE_OK_LOAD_PERMANENTLY = 256  # SQLite >= 3.14.

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
    cdef void sqlite3_result_blob64(sqlite3_context*,const void*, sqlite3_uint64,void(*)(void*))
    cdef void sqlite3_result_double(sqlite3_context*, double)
    cdef void sqlite3_result_error(sqlite3_context*, const char*, int)
    cdef void sqlite3_result_error_toobig(sqlite3_context*)
    cdef void sqlite3_result_error_nomem(sqlite3_context*)
    cdef void sqlite3_result_error_code(sqlite3_context*, int)
    cdef void sqlite3_result_int(sqlite3_context*, int)
    cdef void sqlite3_result_int64(sqlite3_context*, sqlite3_int64)
    cdef void sqlite3_result_null(sqlite3_context*)
    cdef void sqlite3_result_text64(sqlite3_context*, const char*,sqlite3_uint64, void(*)(void*), unsigned char encoding)
    cdef void sqlite3_result_value(sqlite3_context*, sqlite3_value*)

    # Memory management.
    cdef void* sqlite3_malloc(int)
    cdef void sqlite3_free(void *)

    # Misc.
    cdef const char sqlite3_version[]


cdef int SQLITE_CONSTRAINT = 19  # Abort due to constraint violation.

USE_SQLITE_CONSTRAINT = sqlite3_version[:4] >= b'3.26'


cdef extern from "_pysqlite/connection.h":
    ctypedef struct pysqlite_Connection:
        sqlite3 *db


cdef inline unicode decode(key):
    cdef unicode ukey
    if PyBytes_Check(key):
        ukey = key.decode('utf-8')
    elif PyUnicode_Check(key):
        ukey = <unicode>key
    elif key is None:
        return None
    else:
        ukey = unicode(key)
    return ukey


cdef inline bytes encode(key):
    cdef bytes bkey
    if PyUnicode_Check(key):
        bkey = PyUnicode_AsUTF8String(key)
    elif PyBytes_Check(key):
        bkey = <bytes>key
    elif key is None:
        return None
    else:
        bkey = PyUnicode_AsUTF8String(unicode(key))
    return bkey

# Implementation copied from a more up-to-date version in cysqlite.

cdef tuple sqlite_to_python(int argc, sqlite3_value **params):
    cdef:
        int i, vtype
        tuple result = PyTuple_New(argc)

    for i in range(argc):
        vtype = sqlite3_value_type(params[i])
        if vtype == SQLITE_INTEGER:
            pyval = sqlite3_value_int(params[i])
        elif vtype == SQLITE_FLOAT:
            pyval = sqlite3_value_double(params[i])
        elif vtype == SQLITE_TEXT:
            pyval = PyUnicode_DecodeUTF8(
                <const char *>sqlite3_value_text(params[i]),
                <Py_ssize_t>sqlite3_value_bytes(params[i]), NULL)
        elif vtype == SQLITE_BLOB:
            pyval = PyBytes_FromStringAndSize(
                <const char *>sqlite3_value_blob(params[i]),
                <Py_ssize_t>sqlite3_value_bytes(params[i]))
        elif vtype == SQLITE_NULL:
            pyval = None
        else:
            pyval = None

        Py_INCREF(pyval)
        PyTuple_SET_ITEM(result, i, pyval)

    return result

cdef python_to_sqlite(sqlite3_context *context, param):
    cdef:
        bytes tmp
        char *buf
        Py_ssize_t nbytes

    if param is None:
        sqlite3_result_null(context)
    elif isinstance(param, int):
        sqlite3_result_int64(context, <sqlite3_int64>param)
    elif isinstance(param, float):
        sqlite3_result_double(context, <double>param)
    elif isinstance(param, unicode):
        tmp = PyUnicode_AsUTF8String(param)
        PyBytes_AsStringAndSize(tmp, &buf, &nbytes)
        sqlite3_result_text64(context, buf,
                              <sqlite3_uint64>nbytes,
                              <sqlite3_destructor_type>-1,
                              SQLITE_UTF8)
    elif isinstance(param, bytes):
        PyBytes_AsStringAndSize(<bytes>param, &buf, &nbytes)
        sqlite3_result_blob64(context, <void *>buf,
                              <sqlite3_uint64>nbytes,
                              <sqlite3_destructor_type>-1)
    else:
        sqlite3_result_error(
            context,
            encode('Unsupported type %s' % type(param)),
            -1)
        return SQLITE_ERROR

    return SQLITE_OK


cdef inline check_connection(pysqlite_Connection *conn):
    if not conn.db:
        raise IOError('Cannot operate on a closed database!')


# The cysqlite_vtab struct embeds the base sqlite3_vtab struct, and adds a
# field to store a reference to the Python implementation.
ctypedef struct cysqlite_vtab:
    sqlite3_vtab base
    void *table_func_cls


# Like cysqlite_vtab, the cysqlite_cursor embeds the base sqlite3_vtab_cursor
# and adds fields to store references to the current index, the Python
# implementation, the current rows' data, and a flag for whether the cursor has
# been exhausted.
ctypedef struct cysqlite_cursor:
    sqlite3_vtab_cursor base
    long long idx
    void *table_func
    void *row_data
    bint stopped


# We define an xConnect function, but leave xCreate NULL so that the
# table-function can be called eponymously.
cdef int cyConnect(sqlite3 *db, void *pAux, int argc, const char *const*argv,
                   sqlite3_vtab **ppVtab, char **pzErr) with gil:
    cdef:
        int rc
        object table_func_cls = <object>pAux
        cysqlite_vtab *pNew = <cysqlite_vtab *>0

    rc = sqlite3_declare_vtab(
        db,
        encode('CREATE TABLE x(%s);' %
               table_func_cls.get_table_columns_declaration()))
    if rc == SQLITE_OK:
        pNew = <cysqlite_vtab *>sqlite3_malloc(sizeof(pNew[0]))
        memset(<char *>pNew, 0, sizeof(pNew[0]))
        ppVtab[0] = &(pNew.base)

        pNew.table_func_cls = <void *>table_func_cls
        Py_INCREF(table_func_cls)

    return rc


cdef int cyDisconnect(sqlite3_vtab *pBase) with gil:
    cdef:
        cysqlite_vtab *pVtab = <cysqlite_vtab *>pBase
        object table_func_cls = <object>(pVtab.table_func_cls)

    Py_DECREF(table_func_cls)
    sqlite3_free(pVtab)
    return SQLITE_OK


# The xOpen method is used to initialize a cursor. In this method we
# instantiate the TableFunction class and zero out a new cursor for iteration.
cdef int cyOpen(sqlite3_vtab *pBase, sqlite3_vtab_cursor **ppCursor) with gil:
    cdef:
        cysqlite_vtab *pVtab = <cysqlite_vtab *>pBase
        cysqlite_cursor *pCur = <cysqlite_cursor *>0
        object table_func_cls = <object>pVtab.table_func_cls

    pCur = <cysqlite_cursor *>sqlite3_malloc(sizeof(pCur[0]))
    memset(<char *>pCur, 0, sizeof(pCur[0]))
    ppCursor[0] = &(pCur.base)
    pCur.idx = 0
    try:
        table_func = table_func_cls()
    except:
        if table_func_cls.print_tracebacks:
            traceback.print_exc()
        sqlite3_free(pCur)
        return SQLITE_ERROR

    Py_INCREF(table_func)
    pCur.table_func = <void *>table_func
    pCur.stopped = False
    return SQLITE_OK


cdef int cyClose(sqlite3_vtab_cursor *pBase) with gil:
    cdef:
        cysqlite_cursor *pCur = <cysqlite_cursor *>pBase
        object table_func = <object>pCur.table_func
    Py_DECREF(table_func)
    sqlite3_free(pCur)
    return SQLITE_OK


# Iterate once, advancing the cursor's index and assigning the row data to the
# `row_data` field on the cysqlite_cursor struct.
cdef int cyNext(sqlite3_vtab_cursor *pBase) with gil:
    cdef:
        cysqlite_cursor *pCur = <cysqlite_cursor *>pBase
        object table_func = <object>pCur.table_func
        tuple result

    if pCur.row_data:
        Py_DECREF(<tuple>pCur.row_data)

    pCur.row_data = NULL
    try:
        result = tuple(table_func.iterate(pCur.idx))
    except StopIteration:
        pCur.stopped = True
    except:
        if table_func.print_tracebacks:
            traceback.print_exc()
        return SQLITE_ERROR
    else:
        Py_INCREF(result)
        pCur.row_data = <void *>result
        pCur.idx += 1
        pCur.stopped = False

    return SQLITE_OK


# Return the requested column from the current row.
cdef int cyColumn(sqlite3_vtab_cursor *pBase, sqlite3_context *ctx,
                  int iCol) with gil:
    cdef:
        bytes bval
        cysqlite_cursor *pCur = <cysqlite_cursor *>pBase
        sqlite3_int64 x = 0
        tuple row_data

    if iCol == -1:
        sqlite3_result_int64(ctx, <sqlite3_int64>pCur.idx)
        return SQLITE_OK

    if not pCur.row_data:
        sqlite3_result_error(ctx, encode('no row data'), -1)
        return SQLITE_ERROR

    row_data = <tuple>pCur.row_data
    return python_to_sqlite(ctx, row_data[iCol])


cdef int cyRowid(sqlite3_vtab_cursor *pBase, sqlite3_int64 *pRowid):
    cdef:
        cysqlite_cursor *pCur = <cysqlite_cursor *>pBase
    pRowid[0] = <sqlite3_int64>pCur.idx
    return SQLITE_OK


# Return a boolean indicating whether the cursor has been consumed.
cdef int cyEof(sqlite3_vtab_cursor *pBase):
    cdef:
        cysqlite_cursor *pCur = <cysqlite_cursor *>pBase
    return 1 if pCur.stopped else 0


# The filter method is called on the first iteration. This method is where we
# get access to the parameters that the function was called with, and call the
# TableFunction's `initialize()` function.
cdef int cyFilter(sqlite3_vtab_cursor *pBase, int idxNum,
                  const char *idxStr, int argc, sqlite3_value **argv) with gil:
    cdef:
        cysqlite_cursor *pCur = <cysqlite_cursor *>pBase
        object table_func = <object>pCur.table_func
        dict query = {}
        int idx
        int value_type
        tuple row_data
        void *row_data_raw

    if not idxStr or argc == 0 and len(table_func.params):
        return SQLITE_ERROR
    elif len(idxStr):
        params = decode(idxStr).split(',')
    else:
        params = []

    py_values = sqlite_to_python(argc, argv)

    for idx, param in enumerate(params):
        value = argv[idx]
        if not value:
            query[param] = None
        else:
            query[param] = py_values[idx]

    try:
        table_func.initialize(**query)
    except:
        if table_func.print_tracebacks:
            traceback.print_exc()
        return SQLITE_ERROR

    pCur.stopped = False
    try:
        row_data = tuple(table_func.iterate(0))
    except StopIteration:
        pCur.stopped = True
    except:
        if table_func.print_tracebacks:
            traceback.print_exc()
        return SQLITE_ERROR
    else:
        Py_INCREF(row_data)
        pCur.row_data = <void *>row_data
        pCur.idx += 1
    return SQLITE_OK


# SQLite will (in some cases, repeatedly) call the xBestIndex method to try and
# find the best query plan.
cdef int cyBestIndex(sqlite3_vtab *pBase, sqlite3_index_info *pIdxInfo) \
        with gil:
    cdef:
        int i
        int idxNum = 0, nArg = 0
        cysqlite_vtab *pVtab = <cysqlite_vtab *>pBase
        object table_func_cls = <object>pVtab.table_func_cls
        sqlite3_index_constraint *pConstraint = <sqlite3_index_constraint *>0
        list columns = []
        char *idxStr
        int nParams = len(table_func_cls.params)

    for i in range(pIdxInfo.nConstraint):
        pConstraint = pIdxInfo.aConstraint + i
        if not pConstraint.usable:
            continue
        if pConstraint.op != SQLITE_INDEX_CONSTRAINT_EQ:
            continue

        columns.append(table_func_cls.params[pConstraint.iColumn -
                                             table_func_cls._ncols])
        nArg += 1
        pIdxInfo.aConstraintUsage[i].argvIndex = nArg
        pIdxInfo.aConstraintUsage[i].omit = 1

    if nArg > 0 or nParams == 0:
        if nArg == nParams:
            # All parameters are present, this is ideal.
            pIdxInfo.estimatedCost = <double>1
            pIdxInfo.estimatedRows = 10
        else:
            # Penalize score based on number of missing params.
            pIdxInfo.estimatedCost = <double>10000000000000 * <double>(nParams - nArg)
            pIdxInfo.estimatedRows = 10 ** (nParams - nArg)

        # Store a reference to the columns in the index info structure.
        joinedCols = encode(','.join(columns))
        idxStr = <char *>sqlite3_malloc((len(joinedCols) + 1) * sizeof(char))
        memcpy(idxStr, <char *>joinedCols, len(joinedCols))
        idxStr[len(joinedCols)] = b'\x00'
        pIdxInfo.idxStr = idxStr
        pIdxInfo.needToFreeIdxStr = 0
    elif USE_SQLITE_CONSTRAINT:
        return SQLITE_CONSTRAINT
    else:
        pIdxInfo.estimatedCost = DBL_MAX
        pIdxInfo.estimatedRows = 100000
    return SQLITE_OK


cdef class _TableFunctionImpl(object):
    cdef:
        sqlite3_module module
        object table_function

    def __cinit__(self, table_function):
        self.table_function = table_function

    cdef create_module(self, pysqlite_Connection* conn):
        check_connection(conn)

        cdef:
            bytes name = encode(self.table_function.name)
            sqlite3 *db = conn.db
            int rc

        # Populate the SQLite module struct members.
        self.module.iVersion = 0
        self.module.xCreate = NULL
        self.module.xConnect = cyConnect
        self.module.xBestIndex = cyBestIndex
        self.module.xDisconnect = cyDisconnect
        self.module.xDestroy = NULL
        self.module.xOpen = cyOpen
        self.module.xClose = cyClose
        self.module.xFilter = cyFilter
        self.module.xNext = cyNext
        self.module.xEof = cyEof
        self.module.xColumn = cyColumn
        self.module.xRowid = cyRowid
        self.module.xUpdate = NULL
        self.module.xBegin = NULL
        self.module.xSync = NULL
        self.module.xCommit = NULL
        self.module.xRollback = NULL
        self.module.xFindFunction = NULL
        self.module.xRename = NULL

        # Create the SQLite virtual table.
        rc = sqlite3_create_module(
            db,
            <const char *>name,
            &self.module,
            <void *>(self.table_function))

        Py_INCREF(self)

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
    print_tracebacks = True
    _ncols = None

    @classmethod
    def register(cls, conn):
        cdef _TableFunctionImpl impl = _TableFunctionImpl(cls)
        impl.create_module(<pysqlite_Connection *>conn)
        cls._ncols = len(cls.columns)

    def initialize(self, **filters):
        raise NotImplementedError

    def iterate(self, idx):
        raise NotImplementedError

    @classmethod
    def get_table_columns_declaration(cls):
        cdef list accum = []

        for column in cls.columns:
            if isinstance(column, tuple):
                if len(column) != 2:
                    raise ValueError('Column must be either a string or a '
                                     '2-tuple of name, type')
                accum.append('%s %s' % column)
            else:
                accum.append(column)

        for param in cls.params:
            accum.append('%s HIDDEN' % param)

        return ', '.join(accum)
