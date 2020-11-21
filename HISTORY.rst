=======
History
=======

0.1.0 (2020-04-28)
------------------

* First release on PyPI.

0.5.0 (2020-11-09)
------------------

* Add valuation data
* Update omicron to 0.2.0
* Update jq-adaptor to 0.2.1

0.6.0 (2020-11-20)
-------------------

* 重构了``omega.app::start``接口，允许从父进程继承`cfg`设置
* 改用Poetry来进行依赖管理、版本号管理和打包发布
* web interface增加``get_version``接口。此接口也可以用来判断Omega服务器是否在线
