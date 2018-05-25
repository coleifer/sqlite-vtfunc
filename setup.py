import glob
import os
import warnings

from distutils.core import setup, Extension
try:
    from Cython.Build import cythonize
except ImportError:
    cython_installed = False
    warnings.warn('Cython not installed, using pre-generated C source file.')
else:
    cython_installed = True

if cython_installed:
    python_source = 'vtfunc.pyx'
else:
    python_source = 'vtfunc.c'
    cythonize = lambda obj: obj

extension = Extension(
    'vtfunc',
    define_macros=[('MODULE_NAME', '"vtfunc"')],
    libraries=['sqlite3'],
    sources=[python_source])

setup(
    name='vtfunc',
    version='0.3.4',
    description='Tabular user-defined functions for SQLite3.',
    url='https://github.com/coleifer/sqlite-vtfunc',
    dependency_links=[
        'https://github.com/coleifer/pysqlite/zipball/master#egg=pysqlite',
    ],
    author='Charles Leifer',
    author_email='',
    ext_modules=cythonize([extension]),
)
