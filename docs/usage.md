
# 1. 配置文件位置
如果您当前工作在开发环境(DEV)下，则Omega会从代码所处的位置下的config文件夹下加载配置。
如果您当前工作在测试环境（TEST)下，则Omega会从~/.zillionare/omega/config下加载配置。
如果您当前工作在生产环境（PRODUCTION)下，则Omega会从~/zillionare/omega/config下加载配置。

# 2. 修改同步设置

在这一版的安装程序中，没有提供同步设置定制化的步骤。您可以打开配置文件(defaults.yaml)，自行修改:

```yaml
    omega:
        home: ~/zillionare/omgea
        urls:
            checksum: http://stock.jieyu.ai/chksum
            quotes_server: http://localhost:3181
        heartbeat: 10
        sync:
            security_list: 02:00
            calendar: 02:00
            bars:
                - frame: 1d
                    start: 2018-1-1
                    delay: 0 # delay * seconds after the frame is done
                    type:
                    - stock # stock | index
```

同步设置在omega.sync下。各项含义如下：

1. security_list: 在何时同步证券列表。默认为午夜2时。
2. calendar: 在同时同步交易日历。默认为午夜2时。

bars下是行情数据同步的选项。支持的frame有1d（日线）, 60m（60分钟线，以下类推）等。您需要根据redis所能使用的内存数，来设置start的时间。为保证正确获取行情数据，您可以通过delay来设置在数据产生一定时间之后，再去同步行情数据。

type选项中，支持的类型有stock和index。

# 3. 管理omega

1. 要启动Omega的行情服务，请在命令行下输入:

```bash
    omega start
```

1. 行情同步等任务是由jobs进程管理的，所以您还需要启动jobs进程

```bash
    omega start jobs
```

3. 要查看当前有哪些fetcher和jobs进程在运行，使用下面的命令：

```bash
    omega status
```

4. 此外，Omega还提供了stop和restart命令:

```bash

    omega stop jobs
    omega stop
    omega restart jobs
    omega restart
```

# 4. 使用行情数据

虽然Omega提供了HTTP接口，但因为性能优化的原因，其通过HTTP接口提供的数据，都是二进制的。

使用行情数据的正确方式是通过Omicron SDK来访问数据。请跳转至 [Omicron帮助文档](https://zillionare-omicron.readthedocs.io) 继续阅读。
