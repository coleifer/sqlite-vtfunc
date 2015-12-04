import glob
import os

from distutils.core import setup, Extension
try:
    from Cython.Build import cythonize
except ImportError:
    import warnings
    raise RuntimeError('Cython must be installed to build vtfunc.')

python_source = 'vtfunc.pyx'
pysqlite = glob.glob('pysqlite/*.c')

extension = Extension(
    'vtfunc',
    define_macros=[('MODULE_NAME', '"vtfunc"')],
    libraries=['python2.7', 'sqlite3'],
    sources=[python_source] + pysqlite)

setup(
    name='vtfunc',
    version='0.1.0',
    description='',
    url='https://github.com/coleifer/sqlite-vtfunc',
    author='Charles Leifer',
    author_email='',
    ext_modules=cythonize(extension),
)
