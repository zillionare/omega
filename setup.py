#!/usr/bin/env python
"""The setup script."""
import os
import pathlib

import pkg_resources
from setuptools import find_packages, setup
from setuptools.command.install import install

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = history_file.read()

opts = {}

requirements = [
    "apscheduler==3.6.3",
    "arrow>=0.15.5",
    "cfg4py>=0.6.0",
    "ruamel.yaml==0.16",
    "aioredis==1.3.1",
    "hiredis==1.0.1",
    "pyemit>=version=version='0.5.1'",
    "numpy>=1.18.1",
    "numba==0.49.1",
    "aiohttp==3.6.2",
    "pytz==2020.1",
    "xxhash==1.4.3",
    "zillionare-omicron>=0.2.0",
    "aiocache==0.11.1",
    "sanic==20.3.0",
    "psutil==5.7.0",
    "termcolor==1.1.0",
    "gino",
    "asyncpg",
    "sqlalchemy",
    "sh>=1.13",
]

setup_requirements = ["sh"]

test_requirements = ["omega"]


def post_install():
    import sh

    for item in ["config", "data/chksum"]:
        folder = (pathlib.Path("~/zillionare/omega") / item).expanduser()
        os.makedirs(folder, exist_ok=True)

    dst = pathlib.Path("~/zillionare/omega/config/").expanduser()
    for file in [
        "config/defaults.yaml",
        "config/30-omega-validation.conf",
        "config/31-omega-quickscan.conf",
        "config/32-omega-default.conf",
    ]:
        src = pkg_resources.resource_filename("omega", file)
        sh.cp("-r", src, dst)


class InstallCommand(install):
    def run(self):
        install.run(self)
        post_install()


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
    description="Data fetcher framework for zillionare",
    install_requires=requirements,
    license="MIT license",
    long_description=readme + "\n\n" + history,
    include_package_data=True,
    keywords="omega",
    name="zillionare-omega",
    packages=find_packages(include=["omega", "omega.*"]),
    pacakge_data={"omega": ["config/defaults.yaml", "config/32-omega-default.conf"]},
    setup_requires=setup_requirements,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/zillionare/omega",
    version="0.5.1",
    zip_safe=False,
    cmdclass={"install": InstallCommand},
    entry_points={"console_scripts": ["omega=omega.cli:main"]},
)
