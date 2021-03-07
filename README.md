
![](http://images.jieyu.ai/images/hot/zillionbanner.jpg)

<p align="center">
<a href="https://pypi.python.org/pypi/zillionare-omega">
    <img src="http://img.shields.io/pypi/v/zillionare-omega?color=brightgreen" >
</a>

<a href="https://travis-ci.com/zillionare/omega">
<img src="https://api.travis-ci.com/zillionare/omega.svg?branch=release">
</a>

<a href="https://omega.readthedocs.io/en/latest/?badge=latest">
<img src="https://readthedocs.org/projects/omega/badge/?version=latest">
</a>

<a href="https://pepy.tech/project/zillionare-omega">
<img src="https://pepy.tech/badge/zillionare-omega">
</a>

<a href="https://github.com/psf/black">
<img src="https://img.shields.io/badge/code%20style-black-000000.svg">
</a>

<a href="https://opensource.org/licenses/MIT">
<img src="https://img.shields.io/badge/License-MIT-yellow.svg">
</a>
</p>


高速分布式本地行情服务器


# 简介

Omega为大富翁(Zillionare)智能量化交易平台提供数据服务。它是一个分布式、高性能的行情服务器，核心功能有：

1. 并发对接多个上游数据源，如果数据源还支持多账户和多个并发会话的话，Omega也能充分利用这种能力，从而享受到最快的实时行情。目前官方已提供JoinQuant的数据源适配。

2. 高性能和层次化的数据本地化存储，在最佳性能和存储空间上巧妙平衡。在需要被高频调用的行情数据部分，Omega直接使用Redis存储数据；财务数据一个季度才会变动一次，因而读取频率也不会太高，所以存放在关系型数据库中。这种安排为各种交易风格都提供了最佳计算性能。

3. 优秀的可伸缩部署(scalability)特性。Omega可以根据您对数据吞吐量的需求，按需部署在单机或者多台机器上，从而满足个人、工作室到大型团队的数据需求。

4. 自带数据(Battery included)。我们提供了从2015年以来的30分钟k线以上数据，并且通过CDN进行高速分发。安装好Omega之后，您可以最快在十多分钟内将这样巨量的数据同步到本地数据库。


[帮助文档](https://zillionare-omega.readthedocs.io)

鸣谢
=========

Zillionare-Omega采用以下技术构建:

[Pycharm开源项目支持计划](https://www.jetbrains.com/?from=zillionare-omega)

![](_static/jetbrains-variant-3.svg)
