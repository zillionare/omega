

# 版本历史


## 0.1.0 (2020-04-28)
* First release on PyPI.

## 0.5.0 (2020-11-09)
* 增加市值数据
* Update omicron to 0.2.0
* Update jq-adaptor to 0.2.1

## 0.6.0 (2020-11-25)

* 重构了[omega.app.start][]接口，允许从父进程继承`cfg`设置
* web interface增加[omega.app.get_version][]接口。此接口也可以用来判断Omega服务器是否在线
* 本版本适配zillionare-omicron 0.3和zillionare-omega-adaptors-jq 0.2.4

## 1.0 (2020-?)

first stable release

* 可导入从2015年以来的A股30分钟及以上股票数据。
* 高速行情同步。支持多账号、多session、多进程。
* 向外提供服务时，支持load-balance（需要配置http网关，如nginx)。
* 自动实时行情同步。
* 仅支持JoinQuant作为上游数据源
