#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'apscheduler==3.6.3',
    'arrow==0.15.5',
    'cfg4py>=0.4.0',
    'ruamel.yaml==0.16',
    'aioredis==1.3.1',
    'hiredis==1.0.1',
    'pyemit>=0.4.0',
    'numpy>=1.18.1',
    'Click==7.0',
    'numba==0.49.1',
    'aiohttp==3.6.2',
    'pytz==2019.3'
]

setup_requirements = []

test_requirements = []

setup(
    author="Aaron Yang",
    author_email='code@jieyu.ai',
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="Quotation fetcher adaptor framework for zillionare",
    entry_points={
        'console_scripts': [
            'omega=omega.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='omega',
    name='zillionare-omega',
    packages=find_packages(include=['omega', 'omega.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/zillionare/omega',
    version='0.1.0',
    zip_safe=False,
)
