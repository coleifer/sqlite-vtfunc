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
    #extra_compile_args=['-g'],
    #extra_link_args=['-g'],
    define_macros=[('MODULE_NAME', '"vtfunc"')],
    libraries=['sqlite3'],
    sources=[python_source] + pysqlite)

setup(
    name='vtfunc',
    version='0.2.0',
    description='',
    url='https://github.com/coleifer/sqlite-vtfunc',
    author='Charles Leifer',
    author_email='',
    ext_modules=cythonize(extension),
)
