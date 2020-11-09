#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
管理应用程序生命期、全局对象、任务、全局消息响应
"""
import itertools
import logging
import os
import pathlib
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from subprocess import CalledProcessError, check_call, check_output
from typing import Any, Callable, List, Union

import cfg4py
import fire
import omicron
import pkg_resources
import psutil
import sh
from omicron.core.lang import async_run
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from pyemit import emit
from ruamel.yaml import YAML
from termcolor import colored

import omega.jobs.sync as sync
from omega.config.cfg4py_auto_gen import Config
from omega.core import get_config_dir
from omega.core.sanity import quick_scan
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
git_url = "https://github.com/zillionare/omega"

cfg: Config = cfg4py.get_instance()


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
            msg.append(line[i * 80 : min(len(line), (i + 1) * 80)])
    return "\n".join(msg)


# noinspection PyUnresolvedReferences
def update_config(root_key: str, conf: Any):
    config_file = Path("~/zillionare/omega/config/defaults.yaml").expanduser()
    with open(config_file, "r", encoding="utf-8") as f:
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
        with open(config_file, "w", encoding="utf-8") as f:
            parser = YAML()
            parser.dump(cfg, f)
    except Exception:
        # restore the backup
        sh.mv(config_file.with_suffix(".bak"), config_file)


def redo(prompt, func, choice=None):
    if choice is None:
        choose = input(prompt)
        while choose.upper() not in ["C", "Q", "R"]:
            choose = input(prompt)
    else:
        print(prompt)
        choose = choice

    if choose.upper() == "R":
        try:
            func()
        except EarlyJumpError:
            return
    elif choose.upper() == "C":
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
    is_venv = hasattr(sys, "real_prefix") or (
        hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix
    )

    if is_venv:
        return True

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
    src = Path("~/zillionare/omega/config/32-omega-default.conf").expanduser()
    dst = "/etc/rsyslog.d"

    try:
        print("正在应用新的配置文件,请根据提示给予授权：")
        sh.contrib.sudo.cp(src, dst)
        print("即将重启rsyslog服务，请给予授权：")
        sh.contrib.sudo.service("rsyslog", "restart")
    except Exception as e:
        print(e)
        redo("配置rsyslog失败，请排除错误后重试", config_syslog)


def config_logging():
    msg = """
    请指定日志文件存放位置，默认位置[/var/log/zillionare/]
    """
    folder = get_input(msg, None, "/var/log/zillionare")
    folder = Path(folder).expanduser()

    try:
        if not os.path.exists(folder):
            print("正在创建日志目录，可能需要您授权：")
            sh.contrib.sudo.mkdir(folder, "-p")
            sh.contrib.sudo.chmod(777, folder)
    except Exception as e:
        print(e)
        redo("创建日志目录失败，请排除错误重试，或者重新指定目录", config_logging)

    update_config(
        "logging.handlers.validation_report.filename", str(folder / "validation.log")
    )
    config_syslog()


def config_jq_fetcher():
    msg = """
        Omega需要配置数据获取插件才能工作，当前支持的插件列表有:\\n
        [1] jqdatasdk\\n
        请输入序号开始配置[1]:
    """
    index = input(format_msg(msg))
    if index == "" or index == "1":
        account = input("请输入账号:")
        password = input("请输入密码:")

        config = [
            {
                "name": "jqdatasdk",
                "module": "jqadaptor",
                "parameters": {"account": account, "password": password},
            }
        ]
        update_config("quotes_fetchers", config)
    else:
        redo("请输入正确的序号，C跳过，Q退出", config_jq_fetcher)

    try:
        import jqadaptor as jq  # noqa
    except ModuleNotFoundError:
        check_call(
            [sys.executable, "-m", "pip", "install", "zillionare-omega-adaptors-jq"]
        )


def get_input(
    prompt: str, validation: Union[List, Callable], default: Any, op_hint: str = None
):
    if op_hint is None:
        op_hint = "直接回车接受默认值，忽略此项(C)，退出(Q):"
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

        if value.upper() == "C":
            return None
        elif value == "":
            return default
        elif value == "Q":
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
    frames = {
        "1d": get_input("同步日线数据[1000]", is_number, 1000, op_hint),
        "1w": get_input("同步周线线数据[1000]", is_number, 1000, op_hint),
        "1M": get_input("同步月线数据", is_number, 1000, op_hint),
        "1y": get_input("同步年线数据", is_number, 1000, op_hint),
        "1m": get_input("同步1分钟数据[1000]", is_number, 1000, op_hint),
        "5m": get_input("同步5分钟数据[1000]", is_number, 1000, op_hint),
        "15m": get_input("同步15分钟数据[1000]", is_number, 1000, op_hint),
        "30m": get_input("同步30分钟数据[1000]", is_number, 1000, op_hint),
        "60m": get_input("同步60分钟数据[1000]", is_number, 1000, op_hint),
    }

    sync_time = get_input("设置行情同步时间[15:05]", is_valid_time, "15:05", op_hint)

    frames = {k: v for k, v in frames.items() if v is not None}

    update_config("omega.sync.frames", frames)
    if sync_time:
        update_config("omega.sync.time", sync_time)

    os.makedirs(Path("~/zillionare/omega/data/chksum"), exist_ok=True)
    # for unittest
    return frames, sync_time


def config_redis():
    msg = """
        Zillionare-omega使用Redis作为其数据库。请确认系统中已安装好redis。请根据提示输入Redis
        服务器连接信息。
    """
    print(format_msg(msg))
    host = get_input("请输入Reids服务器域名或者IP地址[localhost]，", None, "localhost")
    port = get_input("请输入Redis服务器端口[6379]，", is_valid_port, 6379)
    password = get_input("请输入Redis服务器密码，", None, None)

    if password:
        cmd = f"redis-main -h {host} -p {port} -a {password} ping".split(" ")
    else:
        cmd = f"redis-main -h {host} -p {port} ping".split(" ")

    try:
        print(f"正在测试Redis连接: {' '.join(cmd)}")
        result = check_output(cmd).decode("utf-8")
        if result.find("PONG") != -1:
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
        update_config("redis.dsn", f"redis://{password}@{host}:{port}")
    else:
        update_config("redis.dsn", f"redis://{host}:{port}")


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

        dst = pathlib.Path("~/zillionare/omega/config/").expanduser()
        os.makedirs(dst, exist_ok=True)

        for file in ["config/defaults.yaml", "config/32-omega-default.conf"]:
            src = pkg_resources.resource_filename("omega", file)
            sh.cp("-r", src, dst)

    config_redis()
    config_logging()
    config_jq_fetcher()
    config_sync()

    print("配置已完成，建议通过supervisor来管理Omega服务，祝顺利开启财富之旅！")


def find_fetcher_processes():
    """查找进程名为app的进程"""
    result = {}
    for p in psutil.process_iter():
        cmd = " ".join(p.cmdline())
        if "omega.app" in cmd and "--impl" in cmd and "--group-id" in cmd:
            # fetchers
            m = re.search(r"--impl=([^\s]+)", cmd)
            impl = m.group(1) if m else ""

            m = re.search(r"--group-id=([^\s]+)", cmd)
            group_id = m.group(1) if m else ""

            group = f"{impl}:{group_id}"
            pids = result.get(group, [])
            pids.append(p.pid)
            result[f"{impl}:{group_id}"] = pids

    return result


def start(service: str = ""):
    """

    Args:
        service: if service is '', then starts fetcher processes.

    Returns:

    """
    print(f"正在启动zillionare-omega {colored(service, 'green')}...")

    server_roles = ["PRODUCTION", "TEST", "DEV"]
    if os.environ.get(cfg4py.envar) not in ["PRODUCTION", "TEST", "DEV"]:
        print(
            f"请设置环境变量{colored(cfg4py.envar, 'red')}为["
            f"{colored(server_roles, 'red')}]之一。"
        )
        sys.exit(-1)

    config_dir = get_config_dir()
    cfg4py.init(config_dir, False)

    if service == "jobs":
        return start_jobs()

    start_fetcher_processes()


def start_fetcher_processes():
    procs = find_fetcher_processes()

    # fetcher processes are started by groups
    for fetcher in cfg.quotes_fetchers:
        impl = fetcher.get("impl")
        groups = fetcher.get("groups")

        for _id, group in enumerate(groups):
            sessions = group.get("sessions")
            started_sessions = procs.get(f"{impl}:{_id}", [])
            if sessions - len(started_sessions) > 0:
                print(f"启动的{impl}实例少于配置要求（或尚未启动），正在启动中。。。")
                # sanic manages sessions, so we have to restart it as a whole
                for pid in started_sessions:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except Exception:
                        pass
                _start_fetcher(impl, _id)

    time.sleep(3)
    show_fetcher_processes()


def show_fetcher_processes():
    procs = find_fetcher_processes()

    if len(procs):
        print("   impl   |   group id | pids")
        for group, pids in procs.items():
            impl, gid = group.split(":")
            print(f"{impl:10}|{' ':5}{gid:2}{' ':5}| {pids}")
    else:
        print("None")


def _start_fetcher(impl: str, group_id: int):
    subprocess.Popen(
        [
            sys.executable,
            "-m",
            "omega.app",
            "start",
            f"--impl={impl}",
            f"--group-id={group_id}",
        ],
        stdout=subprocess.DEVNULL,
    )


def start_jobs():
    subprocess.Popen([sys.executable, "-m", "omega.jobs", "start"])

    retry = 0
    while find_jobs_process() is None and retry < 5:
        print("等待omega.jobs启动中")
        retry += 1
        time.sleep(0.5)
    if retry < 5:
        print("omega.jobs启动成功。")
        return
    else:
        print("omega.jobs启动失败。")
        return


def restart_jobs():
    pid = find_jobs_process()
    if pid is None:
        print("omega.jobs未运行。正在启动中...")
        start_jobs()
    else:
        # 如果omega.jobs已经运行
        stop_jobs()
        start_jobs()


def stop_jobs():
    pid = find_jobs_process()
    retry = 0
    while pid is not None and retry < 5:
        try:
            os.kill(pid, signal.SIGTERM)
            retry += 1
            time.sleep(0.5)
        except Exception:
            pass
        pid = find_jobs_process()


def show_jobs_process():
    pid = find_jobs_process()
    if pid:
        print(pid)
    else:
        print("None")


def find_jobs_process():
    for p in psutil.process_iter():
        cmd = " ".join(p.cmdline())
        if cmd.find("-m omega.jobs") != -1:
            return p.pid
    return None


def stop_fetcher_processes():
    retry = 0
    while retry < 5:
        procs = find_fetcher_processes()
        if len(procs) == 0:
            return

        for group, pids in procs.items():
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    pass
        time.sleep(1)


def status():
    print(f"正在运行中的omega-fetchers进程：\n{'=' * 40}")
    show_fetcher_processes()
    print("\n")
    print(f"正在运行中的jobs进程:\n{'=' * 40}")
    show_jobs_process()


def stop(service: str = ""):
    if service == "jobs":
        return stop_jobs()

    stop_fetcher_processes()


@async_run
async def restart(service: str = ""):
    print("正在重启动服务...")
    await _init()

    if service == "jobs":
        return restart_jobs

    stop_fetcher_processes()
    start_fetcher_processes()


@async_run
async def scan():
    await _init()

    # todo: read file location from 31-quickscan.conf
    print("将根据系统配置的同步设置来检查数据完整性，错误日志将写入到/var/log/zillionare/quickscan.log中。")

    counters = await quick_scan()

    print("扫描完成，发现的错误汇总如下：")
    print("frame errors  total")
    print("===== ======  =====")
    for frame, errors in counters.items():
        print(f"{frame:5}", f"{errors[0]:6}", f"{errors[1]:6}")

    await emit.stop()


@async_run
async def sync_sec_list():
    await _init()

    await sync.trigger_single_worker_sync("security_list")


@async_run
async def sync_calendar():
    await _init()
    await sync.trigger_single_worker_sync("calendar")


@async_run
async def sync_bars(frame: str = None, codes: str = None):
    """read sync config from config file, if frame/codes is not provided

    Args:
        frame:
        codes:

    Returns:

    """
    await _init()

    if frame:
        frame_type = FrameType(frame)
        params = sync.read_sync_params(FrameType(frame))
        if codes:
            params["secs"] = list(map(lambda x: x.strip(" "), codes.split(",")))
        await sync.trigger_bars_sync(frame_type, params, force=True)
        logger.info("request %s,%s send to workers.", params, codes)
    else:
        for frame_type in itertools.chain(tf.day_level_frames, tf.minute_level_frames):
            params = sync.read_sync_params(frame_type)
            if params:
                await sync.trigger_bars_sync(frame_type, params, force=True)

            logger.info("request %s,%s send to workers.", params, codes)


async def _init():
    global cfg

    config_dir = get_config_dir()
    cfg = cfg4py.init(config_dir, False)

    impl = cfg.quotes_fetchers[0]["impl"]
    params = cfg.quotes_fetchers[0]["groups"][0]

    await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)
    await AbstractQuotesFetcher.create_instance(impl, **params)
    await omicron.init(AbstractQuotesFetcher)


def main():
    fire.Fire(
        {
            "start": start,
            "setup": setup,
            "stop": stop,
            "status": status,
            "restart": restart,
            "scan": scan,
            "sync_sec_list": sync_sec_list,
            "sync_calendar": sync_calendar,
            "sync_bars": sync_bars,
        }
    )


if __name__ == "__main__":
    main()
