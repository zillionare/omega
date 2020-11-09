========
omega
========


.. image:: https://img.shields.io/pypi/v/omega.svg
        :target: https://pypi.python.org/pypi/omega

.. image:: https://img.shields.io/travis/zillionare/omega.svg
        :target: https://travis-ci.com/zillionare/omega

.. image:: https://readthedocs.org/projects/omega/badge/?version=latest
        :target: https://omega.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


分布式本地行情服务器


* Free software: MIT license
* Documentation: https://omega.readthedocs.io.



Omega 功能
--------

Omega为 :doc:`zillionare` 智能量化交易平台提供数据服务。它是一个分布式、高性能的行情服务器，核心功能有：

1. 可插拔的协议适配，可支持多种数据源。当前官方提供了JoinQuant的行情适配器，未来我们将根据数据源的质量，提供更多的适配；您也可以自行适配其它数据源。

2. 高性能、层级式的数据本地化方案。在需要高频调用的行情数据部分，我们直接使用Redis存储数据；而对每个季度才变动一次的财务数据，则存放在关系型数据库中。这种安排，为初学者、以动量策略为主的交易者提供了较为平滑的学习曲线：您可以从一台机器、仅安装配置Redis服务器的情况下开始量化交易；一旦业务发展、团队扩张，或者需要提取财务因子参与计算，则可以按需启用关系型数据库。

3. 高内聚、低耦合。系统将数据访问与其它功能（行情数据收发、多进程协作、数据校验）等分开，从而使得Omega可以较容易扩展开来。

4. 支持基于cli和Web API的管理。



Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
