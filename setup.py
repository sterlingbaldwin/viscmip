import sys
import os
from setuptools import find_packages, setup
from viscmip.version import __version__

setup(
    name="viscmip",
    version=__version__,
    author="Sterling Baldwin",
    author_email="baldwin32@llnl.gov",
    description="plot and animate a CMIP6 file tree",
    entry_points={'console_scripts':
                  ['viscmip = viscmip.__main__:main']},
    packages=['viscmip'],
    package_dir={'viscmip': 'viscmip'},
    package_data={'viscmip': ['LICENSE']},
    include_package_data=True)
