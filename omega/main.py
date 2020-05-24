#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    管理应用程序生命期、全局对象、任务、全局消息响应
        """
import asyncio
import logging
import os
import pathlib
import platform
import re
import signal
import sys
from pathlib import Path
from subprocess import CalledProcessError, check_output
from typing import Any

import fire
import pkg_resources
import sh
from ruamel.yaml import YAML

logger = logging.getLogger(__name__)
git_url = 'https://github.com/zillionare/omega'


class EarlyJumpError(BaseException):
    pass


def show(msg):
    msg = re.sub(r"^\s*", "", msg)
    msg = re.sub(r"\s*$", "", msg)
    msg = re.sub(r"[\t\n]", "", msg)

    msg = msg.replace("\\t", "\t").replace("\\n", "\n")
    lines = int(len(msg)/80) + 1
    for i in range(lines):
        print(msg[i*80:min(len(msg), (i+1) * 80)])



def update_config(root_key: str, conf: Any):
    config_file = Path('~/zillionare/omega/config/defaults.yaml').expanduser()
    with open(config_file, "r", encoding='utf-8') as f:
        parser = YAML()
        cfg = parser.load(f)
        _cfg = cfg

        keys = root_key.split(".")
        for key in keys[:-1]:
            v = _cfg.get(key, {})
            if len(v) == 0:
                _cfg[key] = {}
            _cfg = _cfg[key]

        if isinstance(conf, dict):
            if _cfg.get(keys[-1]):
                _cfg[keys[-1]].update(conf)
            else:
                _cfg[keys[-1]] = conf
        else:
            _cfg[keys[-1]] = conf

    with open(config_file, "w", encoding='utf-8') as f:
        parser = YAML()
        parser.dump(cfg, f)


def redo(prompt, func, choice=None):
    if choice is None:
        choose = input(prompt)
        while choose.upper() not in ['C', 'Q', 'R']:
            choose = input(prompt)
    else:
        print(prompt)
        choose = choice

    if choose.upper() == 'R':
        try:
            func()
        except EarlyJumpError:
            return
    elif choose.upper() == 'C':
        print("您选择了忽略错误继续安装。")
        raise EarlyJumpError
    else:
        print("您选择了中止安装。")
        sys.exit(-1)


def is_number(x):
    try:
        int(x)
        return True
    except Exception:
        return False


def is_valid_time(tm: str) -> bool:
    try:
        hour, minute = map(int, tm.split(":"))
        if 0 <= hour <= 24 and 0 <= minute <= 60:
            return True
    except Exception:
        return False


def is_in_venv():
    # 是否为virtual env
    is_venv = (hasattr(sys, 'real_prefix') or
               (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix))

    if is_venv: return True

    # 是否为conda
    return os.environ.get("CONDA_DEFAULT_ENV") is not None


def is_valid_port(port):
    try:
        port = int(port)
        if 1000 < port < 65535:
            return True
    except Exception:
        return False


def config_syslog():
    msg = """
    当使用多个工作者进程时，omega需要使用rsyslog作为日志输出设备。请确保rsyslog已经安装并能
    正常工作。如果一切准备就绪，请按回车键继续设置：
    """
    show(msg)
    # wait user's confirmation
    input()
    src = Path('~/zillionare/omega/config/51-omega.conf').expanduser()
    dst = '/etc/rsyslog.d'

    show("正在应用新的配置文件,请根据提示给予授权。")
    try:
        sh.contrib.sudo.cp(src, dst)
        sh.contrib.sudo.service('rsyslog restart')
    except Exception as e:
        print(e)
        redo("配置rsyslog失败，请排除错误后重试", config_syslog)


def config_logging():
    msg = """
    请指定日志文件存放位置，默认位置[/var/log/zillionare/]
    """
    folder = get_input(msg, None, '/var/log/zillionare')
    folder = Path(folder).expanduser()

    try:
        if not os.path.exists(folder):
            print("正在创建日志目录，可能需要您授权：")
            sh.contrib.sudo.mkdir(folder, '-p')
            sh.contrib.sudo.chmod(777, folder)
    except Exception as e:
        print(e)
        redo("创建日志目录失败，请排除错误重试，或者重新指定目录", config_logging)

    update_config('logging.handlers.validation_report.filename',
                  folder / 'validation.log')
    config_syslog()


def config_jq_fetcher():
    msg = """
        Omega需要配置数据获取插件才能工作，当前支持的插件列表有:
        [1] jqdatasdk
        请输入序号开始配置[1]:
    """
    index = input(msg)
    if index == '' or index == '1':
        account = input("请输入账号")
        password = input("请输入密码")

        config = [{
            'name':       'jqdatasdk',
            'module':     'jqadaptor',
            'parameters': {
                'account':  account,
                'password': password
            }
        }]
        update_config('quotes_fetchers', config)
    else:
        redo("请输入正确的序号，C跳过，Q退出", config_jq_fetcher)

    try:
        import jqadaptor as jq
    except ModuleNotFoundError:
        sh.pip('install', 'jqadaptor')


def get_input(prompt: str, validation_func: callable, default: Any,
              op_hint: str = None):
    op_hint = op_hint or "\\n直接回车接受默认值，忽略错误继续(C)，退出(Q):"
    show(prompt + op_hint)
    value = input()

    validation_func = validation_func or (lambda x: True)
    while True:
        if value.upper() == 'C':
            return None
        elif value == '':
            return default
        elif value.upper() == 'Q':
            print("您选择了退出")
            sys.exit(-1)
        elif validation_func(value):
            if isinstance(default, int):
                return int(value)
            return value
        else:
            value = input(prompt + op_hint)


def config_sync():
    msg = """
    请根据提示配置哪些k线数据需要同步到本地数据库。
    \\n提示：
    \\n\\t存储4年左右（1000 bars）A股日线数据大约需要500MB的内存。建议始终同步月线数据和年线数
    据，这些数据样本较少，占用内存少
    """
    show(msg)

    op_hint = ",直接回车接受默认值, 不同步(C)，退出(Q):"
    frames = {'1d':  get_input("同步日线数据[1000]", is_number, 1000, op_hint),
              '1w':  get_input("同步周线线数据[1000]", is_number, 1000, op_hint),
              '1M':  get_input("同步月线数据", is_number, 1000, op_hint),
              '1y':  get_input("同步年线数据", is_number, 1000, op_hint),
              '1m':  get_input('同步1分钟数据[1000]', is_number, 1000, op_hint),
              '5m':  get_input('同步5分钟数据[1000]', is_number, 1000, op_hint),
              '15m': get_input('同步15分钟数据[1000]', is_number, 1000, op_hint),
              '30m': get_input('同步30分钟数据[1000]', is_number, 1000, op_hint),
              '60m': get_input('同步60分钟数据[1000]', is_number, 1000, op_hint)}

    sync_time = get_input('设置行情同步时间[15:05]', is_valid_time, '15:05', op_hint)

    frames = {k:v for k,v in frames.items() if v is not None}

    update_config('omega.sync.frames', frames)
    if sync_time:
        update_config('omega.sync.time', sync_time)

    # for unittest
    return frames, sync_time


def config_redis():
    msg = """
        Zillionare（大富翁）\\n
        ------------------\\n
        Zillionare-omega使用Redis作为其数据库。请确认系统中已安装好redis。请根据提示输入Redis
        服务器连接信息。
    """
    show(msg)
    host = get_input("请输入Reids服务器域名或者IP地址，默认值[localhost]", None, 'localhost')
    port = get_input("请输入Redis服务器端口[6379]", is_valid_port, 6379)
    password = get_input("请输入Redis服务器密码", None, None)

    if password:
        cmd = f"redis-cli -h {host} -p {port} -a {password} ping".split(" ")
    else:
        cmd = f"redis-cli -h {host} -p {port} ping".split(" ")

    try:
        print(f"正在测试Redis连接: {' '.join(cmd)}")
        result = check_output(cmd).decode('utf-8')
        if result.find('PONG') != -1:
            print("连接成功！")
        else:
            print(f"测试返回结果为:{result}")
            msg = "输入的连接信息有误。忽略错误继续安装[C](default)，重新输入(R),退出安装(Q):"
            redo(msg, config_redis)
    except EarlyJumpError:
        return
    except FileNotFoundError:
        print("未在本机找到命令redis-cli，无法运行检测。安装程序将继续。")
    except CalledProcessError as e:
        print(f"错误信息:{e}")
        msg = "无法连接指定服务器。忽略错误继续安装[C](default)，重新输入(R),退出安装(Q):"
        redo(msg, config_redis)

    if password:
        update_config('redis.dsn', f"redis://{password}@{host}:{port}")
    else:
        update_config('redis.dsn', f"redis://{host}:{port}")


def setup(reset_factory=False):
    if not is_in_venv():
        msg = """
            检测到当前未处于任何虚拟环境中。运行Zillionare的正确方式是为其创建单独的虚拟运行环境。
            建议您通过conda或者venv来为Zillionare-omega创建单独的运行环境。
        """
        show(msg)

    if reset_factory:
        import sh

        dst = pathlib.Path('~/zillionare/omega/config/').expanduser()
        os.makedirs(dst, exist_ok=True)

        for file in ['config/defaults.yaml', 'config/51-omega.conf']:
            src = pkg_resources.resource_filename('omega', file)
            sh.cp("-r", src, dst)

    config_redis()
    config_logging()
    config_jq_fetcher()
    config_sync()


async def main():
    from omega.app import Application

    app = Application()
    await app.start()


def start():
    from omega import app_name

    logger.info("starting zillionare %s main process...", app_name)
    if platform.system() in "Linux":
        try:
            import uvloop

            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        except ModuleNotFoundError:
            logger.warning(
                'uvloop is required for better performance, continuing with '
                'degraded '
                'service.')

    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()

def cli():
    fire.Fire({
        'start': start,
        'setup': setup
    })


if __name__ == "__main__":
    cli()
