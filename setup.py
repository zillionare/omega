#!/usr/bin/env python

"""The setup script."""
import os
import pathlib

import pkg_resources
from setuptools import setup, find_packages
from setuptools.command.install import install

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

opts = {}

requirements = [
    'apscheduler==3.6.3',
    'arrow==0.15.5',
    'cfg4py>=0.4.0',
    'ruamel.yaml==0.16',
    'aioredis==1.3.1',
    'hiredis==1.0.1',
    'pyemit>=0.4.0',
    'numpy>=1.18.1',
    'numba==0.49.1',
    'aiohttp==3.6.2',
    'pytz==2019.3',
    'xxhash==1.4.3',
    'omicron==0.1.1'
]

setup_requirements = []

test_requirements = []


def post_install():
    import sh

    for item in ['config', 'data/chksum']:
        folder = (pathlib.Path('~/zillionare/omega')/item).expanduser()
        os.makedirs(folder, exist_ok=True)

    dst = pathlib.Path('~/zillionare/omega/config/').expanduser()
    for file in ['config/defaults.yaml', 'config/51-omega.conf']:
        src = pkg_resources.resource_filename('omega', file)
        sh.cp("-r", src, dst)


class InstallCommand(install):
    def run(self):
        install.run(self)
        post_install()


setup(
    author="Aaron Yang",
    author_email='code@jieyu.ai',
    python_requires='>=3.8 ',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Data fetcher framework for zillionare",
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='omega',
    name='zillionare-omega',
    packages=find_packages(include=['omega', 'omega.*']),
    pacakge_data={'omega': ['config/defaults.yaml', 'config/51-omega.conf']},
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/zillionare/omega',
    version='0.4.0',
    zip_safe=False,
    cmdclass={
        'install': InstallCommand
    },
    entry_points={
        'console_scripts': ['omega=omega.main:cli']
    }
)
