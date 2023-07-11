#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-tapfiliate",
    version="1.0.0",
    description="Singer.io target for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_tapfiliate"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    target-tapfiliate=target_tapfiliate:main
    """,
    packages=["target_tapfiliate"],
    package_data = {},
    include_package_data=True,
)
