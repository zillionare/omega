=========
关于Omega
=========


.. image:: https://img.shields.io/pypi/v/omega.svg
        :target: https://pypi.python.org/pypi/omega

.. image:: https://img.shields.io/travis/zillionare/omega.svg
        :target: https://travis-ci.com/zillionare/omega

.. image:: https://readthedocs.org/projects/omega/badge/?version=latest
        :target: https://omega.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

.. image:: https://pepy.tech/badge/zillionare-omega
    :target: https://pepy.tech/project/zillionare-omega

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black

.. image:: https://img.shields.io/badge/License-MIT-yellow.svg
    :target: https://opensource.org/licenses/MIT


高速分布式本地行情服务器


特点
----

Omega为大富翁(Zillionare)智能量化交易平台提供数据服务。它是一个分布式、高性能的行情服务器，核心功能有：

1. 支持并发对接多个上游数据源。如果数据源支持多账户和多个并发会话，您还可以并发使用这些数据连接，从而享受到最快的实时行情。目前官方已提供JoinQuant的数据源适配。

2. 高性能和层次化的数据本地化存储，在最佳性能和存储空间上巧妙平衡。在需要被高频调用的行情数据部分，Omega直接使用Redis存储数据；财务数据一个季度才会变动一次，因而读取频率也不会太高，所以存放在关系型数据库中。这种安排为各种交易风格都提供了最佳计算性能。

3. 优秀的可伸缩部署(scalability)特性。Omega可以在根据您对数据吞吐量的需求，在从一到多台服务器上按需部署，从而满足个人、工作室到大型团队的数据需求。


`[更多] <https://zillionare-omega.readthedocs.io>`_

鸣谢
=========

Zillionare-Omega采用以下技术构建:

    * Pycharm开源项目支持计划_

    .. image:: _static/jetbrains-variant-3.svg
        :target: https://www.jetbrains.com/?from=zillionare-omega

    * Cookiecutter_
    * Cookiecutter-pypackage_


.. _Pycharm开源项目支持计划: https://www.jetbrains.com/?from=zillionare-omega
.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _Cookiecutter-pypackage: https://github.com/audreyr/cookiecutter-pypackage