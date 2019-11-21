#!/usr/bin/env python3
from setuptools import setup, find_packages

import codecs
import os
import re

# Get the version from __init__.py without having to import it.
def _get_version():
    with codecs.open(
        os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "aristoteles", "__init__.py"
        ),
        "r",
    ) as init_py:
        version_match = re.search(
            r"^__version__ = ['\"]([^'\"]*)['\"]", init_py.read(), re.M
        )

        if version_match:
            return version_match.group(1)
        raise RuntimeError("Unable to find version string.")


setup(
    name="aristoteles",
    version=_get_version(),
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        "arrow",
        "click",
        "configobj",
        "h5py >= 2.10.0",
        "numpy >= 1.16",
        "future",
    ],
    author="The CHIME Collaboration",
    entry_points="""
        [console_scripts]
        aristoteles=aristoteles.aristoteles:entry
    """,
    author_email="dvw@phas.ubc.ca",
    description="wview-to-HDF5 converter",
    license="GPL v3.0",
    url="https://github.com/chime-experiment/aristoteles",
)
