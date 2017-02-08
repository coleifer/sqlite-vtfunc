import glob
import os

from distutils.core import setup, Extension
try:
    from Cython.Build import cythonize
except ImportError:
    import warnings
    raise RuntimeError('Cython must be installed to build vtfunc.')

python_source = 'vtfunc.pyx'
extension = Extension(
    'vtfunc',
    define_macros=[('MODULE_NAME', '"vtfunc"')],
    libraries=['sqlite3'],
    sources=[python_source])

setup(
    name='vtfunc',
    version='0.2.2',
    description='Tabular user-defined functions for SQLite3.',
    url='https://github.com/coleifer/sqlite-vtfunc',
    install_requires=['pysqlite>=2.8.2', 'Cython'],
    dependency_links=[
        'https://github.com/coleifer/pysqlite/zipball/master#egg=pysqlite',
    ],
    author='Charles Leifer',
    author_email='',
    ext_modules=cythonize(extension),
)
