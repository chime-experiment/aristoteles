#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(
    name="aristoteles",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    packages=find_packages(),
    zip_safe=False,
    install_requires=[
        "arrow >= 1.0",
        "click",
        "configobj",
        "h5py >= 2.10.0",
        "numpy >= 1.16",
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
