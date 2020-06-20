#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
    管理应用程序生命期、全局对象、任务、全局消息响应
        """
import asyncio
import json
import logging
import os
import pathlib
import re
import signal
import sys
from collections import ChainMap
from pathlib import Path
from subprocess import CalledProcessError, check_output, check_call
from typing import Any, Union, List, Callable

import cfg4py
import fire
import omicron
import pkg_resources
import psutil
import sh
from ruamel.yaml import YAML
from termcolor import colored

from omega.core import get_config_dir
from omega.core.sanity import quick_scan

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
git_url = 'https://github.com/zillionare/omega'

cfg = cfg4py.get_instance()


class EarlyJumpError(BaseException):
    pass


def format_msg(msg):
    msg = re.sub(r"\n\s+", "", msg)
    msg = re.sub(r"[\t\n]", "", msg)

    msg = msg.replace("\\t", "\t").replace("\\n", "\n")
    lines = msg.split("\n")

    msg = []
    for line in lines:
        for i in range(int(len(line) / 80 + 1)):
            msg.append(line[i * 80: min(len(line), (i + 1) * 80)])
    return "\n".join(msg)


# noinspection PyUnresolvedReferences
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

    try:
        sh.cp(config_file, config_file.with_suffix(".bak"))
        with open(config_file, "w", encoding='utf-8') as f:
            parser = YAML()
            parser.dump(cfg, f)
    except Exception as e:
        # restore the backup
        sh.mv(config_file.with_suffix(".bak"), config_file)


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
    # wait user's confirmation
    input(format_msg(msg))
    src = Path('~/zillionare/omega/config/32-omega-default.conf').expanduser()
    dst = '/etc/rsyslog.d'

    try:
        print("正在应用新的配置文件,请根据提示给予授权：")
        sh.contrib.sudo.cp(src, dst)
        print("即将重启rsyslog服务，请给予授权：")
        sh.contrib.sudo.service('rsyslog', 'restart')
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
                  str(folder / 'validation.log'))
    config_syslog()


def config_jq_fetcher():
    msg = """
        Omega需要配置数据获取插件才能工作，当前支持的插件列表有:\\n
        [1] jqdatasdk\\n
        请输入序号开始配置[1]:
    """
    index = input(format_msg(msg))
    if index == '' or index == '1':
        account = input("请输入账号:")
        password = input("请输入密码:")

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
        check_call([sys.executable, '-m', 'pip', 'install',
                    'zillionare-omega-adaptors-jq'])


def get_input(prompt: str, validation: Union[List, Callable], default: Any,
              op_hint: str = None):
    if op_hint is None: op_hint = "直接回车接受默认值，忽略此项(C)，退出(Q):"
    value = input(format_msg(prompt + op_hint))

    while True:
        if isinstance(validation, List) and value.upper() in validation:
            is_valid_input = True
        elif validation is None:
            is_valid_input = True
        elif isinstance(validation, Callable):
            is_valid_input = validation(value)
        else:
            is_valid_input = True

        if value.upper() == 'C':
            return None
        elif value == '':
            return default
        elif value == 'Q':
            print("您选择了退出")
            sys.exit(-1)
        elif is_valid_input:
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
    print(format_msg(msg))

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

    frames = {k: v for k, v in frames.items() if v is not None}

    update_config('omega.sync.frames', frames)
    if sync_time:
        update_config('omega.sync.time', sync_time)

    os.makedirs(Path('~/zillionare/omega/data/chksum'), exist_ok=True)
    # for unittest
    return frames, sync_time


def config_redis():
    msg = """
        Zillionare-omega使用Redis作为其数据库。请确认系统中已安装好redis。请根据提示输入Redis
        服务器连接信息。
    """
    print(format_msg(msg))
    host = get_input("请输入Reids服务器域名或者IP地址[localhost]，", None, 'localhost')
    port = get_input("请输入Redis服务器端口[6379]，", is_valid_port, 6379)
    password = get_input("请输入Redis服务器密码，", None, None)

    if password:
        cmd = f"redis-main -h {host} -p {port} -a {password} ping".split(" ")
    else:
        cmd = f"redis-main -h {host} -p {port} ping".split(" ")

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
        print("未在本机找到命令redis-main，无法运行检测。安装程序将继续。")
    except CalledProcessError as e:
        print(f"错误信息:{e}")
        msg = "无法连接指定服务器。忽略错误继续安装[C](default)，重新输入(R),退出安装(Q):"
        redo(msg, config_redis)

    if password:
        update_config('redis.dsn', f"redis://{password}@{host}:{port}")
    else:
        update_config('redis.dsn', f"redis://{host}:{port}")


def setup(reset_factory=False):
    msg = """
    Zillionare-omega (大富翁)\\n
    -------------------------\\n
    感谢使用! Zillionare(大富翁)是一系列证券分析工具，其中Omega是其中获取行情和其它关键信息、
    数据的组件。\\n
    """
    if not is_in_venv():
        msg = """
            检测到当前未处于任何虚拟环境中。运行Zillionare的正确方式是为其创建单独的虚拟运行环境。
            建议您通过conda或者venv来为Zillionare-omega创建单独的运行环境。
        """
        print(format_msg(msg))

    if reset_factory:
        import sh

        dst = pathlib.Path('~/zillionare/omega/config/').expanduser()
        os.makedirs(dst, exist_ok=True)

        for file in ['config/defaults.yaml', 'config/32-omega-default.conf']:
            src = pkg_resources.resource_filename('omega', file)
            sh.cp("-r", src, dst)

    config_redis()
    config_logging()
    config_jq_fetcher()
    config_sync()

    print("配置已完成，建议通过supervisor来管理Omega服务，祝顺利开启财富之旅！")


def find_procs(app: str):
    """查找进程名为app的进程"""
    pids = []
    for p in psutil.process_iter():
        cmdline = p.cmdline()
        if len(cmdline) >= 3 and cmdline[0] == 'python' and cmdline[2] == app:
            pids.append(p.pid)

    return pids


def start(service: str):
    print(f"正在启动zillionare-omega {colored(service, 'green')}...")

    server_roles = ['PRODUCTION', 'TEST', 'DEV']
    if os.environ.get(cfg4py.envar) not in ['PRODUCTION', 'TEST', 'DEV']:
        print(f"请设置环境变量{colored(cfg4py.envar, 'red')}为["
              f"{colored(server_roles, 'red')}]之一。")
        sys.exit(-1)

    config_dir = get_config_dir()
    cfg4py.init(config_dir, False)

    pids = find_procs(f'omega.{service}')
    if len(pids):
        print(f"服务{service}已经启动,进程为{colored(str(pids), 'green')}")
        choose = get_input("请选择忽略（1）, 重启（2）：", validation=[1, 2], default=1)
        if choose == 2:
            [os.kill(pid, signal.SIGTERM) for pid in pids]
        else:
            sys.exit(0)

    # either pid is invalid or service not started
    # subprocess.Popen(['nohup', sys.executable, '-m', 'omega.jobs', 'start', '&'])
    os.system(f"nohup {sys.executable} -m omega.jobs start")
    # todo: wait until it's really started
    print(f"服务Zillionare-omega/{colored(service, 'green')}已经启动。")


def stop(service: str):
    pids = find_procs(f'omega.{service}')
    for pid in pids:
        try:
            print(f"正在停止进程{colored(pid, 'green')}...")
            os.kill(pid, signal.SIGTERM)
            print(f"进程{pid}已退出.")
        except Exception:
            pass


def scan():
    config_dir = get_config_dir()
    cfg4py.init(config_dir, False)

    frames_to_sync = dict(ChainMap(*cfg.omega.sync.frames))
    print("系统设置自动同步的数据是：")
    print(json.dumps(frames_to_sync, indent=2))

    # todo: read file location from 31-quickscan.conf
    print("错误日志将写入到/var/log/zillionare/quickscan.log中。")

    asyncio.run(omicron.init())
    counters = asyncio.run(quick_scan())
    print("扫描完成，发现的错误汇总如下：")
    print("frame errors  total")
    print("===== ======  =====")
    for frame, errors in counters.items():
        print(f"{frame:5}", f"{errors[0]:6}", f"{errors[1]:6}")

def main():
    fire.Fire({
        'start': start,
        'setup': setup,
        'stop':  stop,
        'scan':  scan
    })


if __name__ == "__main__":
    main()
