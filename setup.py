#!/usr/bin/env python
"""The setup script."""

from setuptools import find_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

opts = {}

requirements = [
    "aiocache==0.11.1",
    "aiohttp==3.7.3",
    "aioredis==1.3.1",
    "apscheduler==3.6.3",
    "arrow==0.15.5",
    "asyncpg==0.21.0",
    "cfg4py==0.8.0",
    "fire==0.3.1",
    "gino==1.0.1",
    "hiredis==1.1.0",
    "numba==0.49.1",
    "numpy>=1.19.4",
    "psutil==5.7.3",
    "pytz>=2020.1",
    "ruamel.yaml==0.16",
    "sanic==20.9.1",
    "sh>=1.13",
    "termcolor==1.1.0",
    "xxhash==1.4.4",
    "zillionare-omicron>=0.3.0",
    "zillionare-omega-adaptors-jq>=0.2.4",
]

setup_requirements = ["sh"]

setup(
    author="Aaron Yang",
    author_email="code@jieyu.ai",
    python_requires=">=3.8 ",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.8",
    ],
    description="分布式本地行情服务框架",
    install_requires=requirements,
    license="MIT license",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="omega",
    name="zillionare-omega",
    packages=find_packages(include=["omega", "omega.*"]),
    setup_requires=setup_requirements,
    url="https://github.com/zillionare/omega",
    version="0.6.0",
    zip_safe=False,
    entry_points={"console_scripts": ["omega=omega.cli:main"]},
)
