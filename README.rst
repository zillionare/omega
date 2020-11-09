========
Omega
========


.. image:: https://img.shields.io/pypi/v/omega.svg
        :target: https://pypi.python.org/pypi/omega

.. image:: https://img.shields.io/travis/zillionare/omega.svg
        :target: https://travis-ci.com/zillionare/omega

.. image:: https://readthedocs.org/projects/omega/badge/?version=latest
        :target: https://omega.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status


分布式本地行情服务框架


* Free software: MIT license



特点
----------

Omega为大富翁(Zillionare)智能量化交易平台提供数据服务。它是一个分布式、高性能的行情服务器，核心功能有：

1. 支持并发对接多个上游数据源。如果数据源支持多账户、账户支持多个并发会话，您还可以并发使用这些数据连接，从而享受到最快的实时行情。
  目前官方已提供JoinQuant的数据源适配。

2. 高性能和层次化的数据本地化存储，在最佳性能和存储空间上巧妙平衡。在需要被高频调用的行情数据部分，Omega直接使用Redis存储数据；财务数据一个季度才会变动一次，因而读取频率也不会太高，所以存放在关系型数据库中。对短线高频量化交易来说，这种安排可以提供最好的数据读取性能。

3. 优秀的可伸缩部署(scalability)特性。Omega可以在根据您对数据吞吐量的需求，在从一到多台服务器上按需部署，从而满足个人、工作室到大型团队的数据需求。



Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
