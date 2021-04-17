#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
管理应用程序生命期、全局对象、任务、全局消息响应
"""
import asyncio
import itertools
import logging
import os
import random
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, List, Union

import aiohttp
import aioredis
import asyncpg
import cfg4py
import fire
import omicron
import psutil
import sh
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from pyemit import emit
from ruamel.yaml import YAML
from termcolor import colored

import omega
from omega.config import get_config_dir
from omega.fetcher import archive
from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher
from omega.jobs import syncjobs

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


class CancelError(BaseException):
    pass


def load_factory_settings():
    config_file = os.path.join(factory_config_dir(), "defaults.yaml")

    with open(config_file, "r") as f:
        parser = YAML()
        _cfg = parser.load(f)

    return _cfg


def factory_config_dir():
    module_dir = os.path.dirname(omega.__file__)
    return os.path.join(module_dir, "config")


def format_msg(msg: str):
    """格式化msg并显示在控制台上

    本函数允许在写代码时按格式要求进行缩进和排版，但在输出时，这些格式都会被移除；对较长的文本，
    按每80个字符为一行进行输出。

    如果需要在msg中插入换行或者制表符，使用`\\n`和`\\t`。
    args:
        msg:

    returns:
    """
    msg = re.sub(r"\n\s+", "", msg)
    msg = re.sub(r"[\t\n]", "", msg)

    msg = msg.replace("\\t", "\t").replace("\\n", "\n")
    lines = msg.split("\n")

    msg = []
    for line in lines:
        for i in range(int(len(line) / 80 + 1)):
            msg.append(line[i * 80 : min(len(line), (i + 1) * 80)])
    return "\n".join(msg)


def update_config(settings: dict, root_key: str, conf: Any):
    keys = root_key.split(".")
    current_item = settings
    for key in keys[:-1]:
        v = current_item.get(key, {})
        current_item[key] = v
        current_item = current_item[key]

    if isinstance(conf, dict):
        if current_item.get(keys[-1]):
            current_item[keys[-1]].update(conf)
        else:
            current_item[keys[-1]] = conf
    else:
        current_item[keys[-1]] = conf


def append_fetcher(settings: dict, worker):
    qf = settings.get("quotes_fetchers", [])
    settings["quotes_fetchers"] = qf

    qf.append(worker)


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
    except (ValueError, Exception):
        return False


async def check_postgres(dsn: str):
    try:
        conn = await asyncpg.connect(dsn=dsn)

        print("连接成功，正在初始化数据库...")
        script_file = os.path.join(factory_config_dir(), "sql/init.sql")
        with open(script_file, "r") as f:
            script = "".join(f.readlines())

            await conn.execute(script)
        return True
    except asyncpg.InvalidCatalogNameError:
        print("数据库<zillionare>不存在，请联系管理员创建后，再运行本程序")
    except asyncpg.InvalidPasswordError:
        print("账号或者密码错误，请重新输入!")
        return False
    except OSError:
        print("无效的地址或者端口")
        return False
    except Exception as e:
        print("出现未知错误。")
        logger.exception(e)
    return False


async def config_postgres(settings):
    """配置数据连接并进行测试"""
    msg = """
        配置数据库并非必须。如果您仅限于在某些场景下使用Zillionare-omega，也可以不配置
        数据库更多信息，\\n请参阅https://readthedocs.org/projects/zillionare-omega/

        \\n跳过此项[S], 任意键继续:
    """
    choice = input(format_msg(msg))
    if choice.upper() == "S":
        return

    action = "R"
    while action == "R":
        host = get_input(
            "请输入服务器地址，", None, os.environ.get("POSTGRES_HOST") or "localhost"
        )
        port = get_input(
            "请输入服务器端口，", is_valid_port, os.environ.get("POSTGRES_PORT") or 5432
        )
        account = get_input("请输入账号,", None, os.environ.get("POSTGRES_USER"))
        password = get_input("请输入密码,", None, os.environ.get("POSTGRES_PASSWORD"))
        dbname = get_input(
            "请输入数据库名,", None, os.environ.get("POSTGRES_DB") or "zillionare"
        )

        print("正在测试Postgres连接...")
        dsn = f"postgres://{account}:{password}@{host}:{port}/{dbname}"
        if await check_postgres(dsn):
            update_config(settings, "postgres.dsn", dsn)
            update_config(settings, "postgres.enabled", True)
            print(f"[{colored('PASS', 'green')}] 数据库连接成功，并成功初始化！")
            return True
        else:
            hint = f"[{colored('FAIL', 'red')}] 忽略错误[C]，重新输入[R]，退出[Q]"
            action = choose_action(hint)


def choose_action(prompt: str, actions: tuple = None, default_action="R"):
    print(format_msg(prompt))
    actions = ("C", "R", "Q")

    answer = input().upper()

    while answer not in actions:
        print(f"请在{actions}中进行选择")
        answer = input().upper()

    if answer == "Q":
        print("您选择了放弃安装。安装程序即将退出")
        sys.exit(0)

    if answer == "C":
        print("您选择了忽略本项。您可以在此后重新运行安装程序，或者手动修改配置文件")

    return answer


def config_logging(settings):
    msg = """
    请指定日志文件存放位置:
    """
    action = "R"
    while action == "R":
        try:
            folder = get_input(msg, None, "/var/log/zillionare")
            folder = Path(folder).expanduser()

            if not os.path.exists(folder):
                try:
                    os.makedirs(folder, exist_ok=True)
                except PermissionError:
                    print("正在创建日志目录，需要您的授权:")
                    sh.contrib.sudo.mkdir(folder, "-p")
                    sh.contrib.sudo.chmod(777, folder)

            logfile = os.path.join(folder, "omega.log")
            update_config(settings, "logreceiver.filename", logfile)
            action = None
        except Exception as e:
            print(e)

            prompt = "创建日志目录失败，请排除错误重试，或者重新指定目录"
            action = choose_action(prompt)

    # activate file logging now
    root_logger = logging.getLogger()
    logfile = os.path.join(folder, "omega.log")
    handler = logging.handlers.RotatingFileHandler(logfile)
    fmt_str = "%(asctime)s %(levelname)-1.1s %(process)d %(name)s:%(funcName)s:%(lineno)s | %(message)s"

    fmt = logging.Formatter(fmt_str)

    handler.setFormatter(fmt)
    root_logger.addHandler(handler)
    logger.info("logging output is written to %s now", logfile)


def config_fetcher(settings):
    """配置jq_fetcher

    为Omega安装jqdatasdk, zillionare-omega-adaptors-jq, 配置jqdata访问账号

    """
    msg = """
        Omega需要配置上游行情服务器。当前支持的上游服务器有:\\n
        [1] 聚宽`<joinquant>`\\n
    """
    print(format_msg(msg))
    more_account = True
    workers = []
    port = 3181
    while more_account:
        account = get_input("请输入账号:", None, os.environ.get("JQ_ACCOUNT") or "")
        password = get_input("请输入密码:", None, os.environ.get("JQ_PASSWORD") or "")
        sessions = get_input("请输入并发会话数", None, 1, "默认值[1]")
        workers.append(
            {
                "account": account,
                "password": password,
                "sessions": sessions,
                "port": port,
            }
        )
        port += 1
        more_account = input("继续配置新的账号[y|N]?\n").upper() == "Y"

    settings["quotes_fetchers"] = []
    append_fetcher(settings, {"impl": "jqadaptor", "workers": workers})


def get_input(
    prompt: str,
    validation: Union[None, List, Callable],
    default: Any,
    op_hint: str = None,
):
    if op_hint is None:
        op_hint = f"忽略此项(C)，退出(Q)，回车选择默认值[{default}]："
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
        elif value.upper() == "Q":
            print("您选择了退出")
            sys.exit(-1)
        elif is_valid_input:
            if isinstance(default, int):
                return int(value)
            return value
        else:
            value = input(prompt + op_hint)


async def check_redis(dsn: str):
    redis = await aioredis.create_redis(dsn)
    await redis.set("omega-test", "delete me on sight")
    redis.close()
    await redis.wait_closed()


async def config_redis(settings):
    msg = """
        Zillionare-omega使用Redis作为其数据库。请确认系统中已安装好redis。\\n请根据提示输入
        Redis服务器连接信息。
    """
    print(format_msg(msg))
    action = "R"
    while action == "R":
        host = get_input(
            "请输入Reids服务器，", None, os.environ.get("REDIS_HOST") or "localhost"
        )
        port = get_input(
            "请输入Redis服务器端口，", is_valid_port, os.environ.get("REDIS_PORT") or 6379
        )
        password = get_input(
            "请输入Redis服务器密码，", None, os.environ.get("REDIS_PASSWORD") or ""
        )

        logger.info("give redis configurations are: %s, %s, %s", host, port, password)
        try:
            if password:
                dsn = f"redis://{password}@{host}:{port}"
            else:
                dsn = f"redis://{host}:{port}"
            await check_redis(dsn)
            print(f"[{colored('PASS', 'green')}] redis连接成功: {dsn}")
            update_config(settings, "redis.dsn", dsn)
            update_config(settings, "logreceiver.dsn", dsn)
            update_config(settings, "logging.handlers.redis.host", host)
            update_config(settings, "logging.handlers.redis.port", port)
            update_config(settings, "logging.handlers.redis.password", password)
            action = None
        except Exception as e:
            logger.exception(e)
            action = choose_action(
                f"[{colored('FAIL', 'red')}]连接失败。忽略错误[C]," f"重输入[R]，放弃安装[Q]"
            )


def print_title(msg):
    print(colored(msg, "green"))
    print(colored("".join(["-"] * len(msg)), "green"))


async def setup(reset_factory=False, force=False):
    """安装初始化入口

    Args:
        reset_factory: reset to factory settings
        force: if true, force setup no matter if run already

    Returns:

    """
    msg = """
    Zillionare-omega (大富翁)\\n
    -------------------------\\n
    感谢使用Zillionare-omega -- 高速分布式行情服务器！\\n
    """

    print(format_msg(msg))

    if not force:
        config_file = os.path.join(get_config_dir(), "defaults.yaml")
        if os.path.exists(config_file):
            print(f"{colored('[PASS]', 'green')} 安装程序已在本机上成功运行")
            sys.exit(0)

    if reset_factory:
        import sh

        dst = get_config_dir()
        os.makedirs(dst, exist_ok=True)

        src = os.path.join(factory_config_dir(), "defaults.yaml")
        dst = os.path.join(get_config_dir(), "defaults.yaml")
        sh.cp("-r", src, dst)

    print_title("Step 1. 检测安装环境...")
    settings = load_factory_settings()

    if not check_environment():
        sys.exit(-1)

    print_title("Step 2. 配置日志")
    config_logging(settings)
    print_title("Step 3. 配置上游服务器")
    config_fetcher(settings)
    print_title("Step 4. 配置Redis服务器")
    await config_redis(settings)
    print_title("Step 5. 配置Postgres服务器")
    await config_postgres(settings)
    save_config(settings)

    print_title("Step 6. 下载历史数据")
    config_dir = get_config_dir()
    cfg4py.init(config_dir, False)
    remove_console_log_handler()

    await start("fetcher")
    await download_archive(None)

    print_title("配置已完成。现在为您启动Omega,开启财富之旅！")

    await start("jobs")
    await status()


def save_config(settings):
    os.makedirs(get_config_dir(), exist_ok=True)
    config_file = os.path.join(get_config_dir(), "defaults.yaml")
    settings["version"] = omega.__version__

    try:
        with open(config_file, "w", encoding="utf-8") as f:
            parser = YAML()
            parser.indent(sequence=4, offset=2)
            parser.dump(settings, f)
    except Exception as e:  # noqa
        # restore the backup
        logger.exception(e)
        logger.warning("failed to save config:\n%s", settings)
        print(f"[{colored('FAIL', 'green')}] 无法保存文件。安装失败。")
        sys.exit(-1)


def check_environment():
    if not is_in_venv():
        msg = """
            检测到当前未处于任何虚拟环境中。\\n运行Zillionare的正确方式是为其创建单独的虚拟运行环境。\\n
            建议您通过conda或者venv来为Zillionare-omega创建单独的运行环境。
        """
        hint = "按任意键忽略错误继续安装，退出安装[Q]:\n"

        prompt = f"[{colored('FAIL','green')}] {msg} \\n{hint}"
        print(format_msg(prompt))
        if input().upper() == "Q":
            print("您选择了终止安装程序")
            sys.exit(0)
    else:
        print(f"[{colored('PASS', 'green')}] 当前运行在虚拟环境下")

    # create /var/log/zillionare for logging
    if not os.path.exists("/var/log/zillionare"):
        sh.contrib.sudo.mkdir("/var/log/zillionare", "-m", "777")
    return True


def find_fetcher_processes():
    """查找所有的omega(fetcher)进程

    Omega进程在ps -aux中显示应该包含 omega.app --impl=&ltfetcher&gt --port=&ltport&gt信息
    """
    result = {}
    for p in psutil.process_iter():
        cmd = " ".join(p.cmdline())
        if "omega.app start" in cmd and "--impl" in cmd and "--port" in cmd:

            m = re.search(r"--impl=([^\s]+)", cmd)
            impl = m.group(1) if m else ""

            m = re.search(r"--port=(\d+)", cmd)
            port = m.group(1) if m else ""

            group = f"{impl}:{port}"
            pids = result.get(group, [])
            pids.append(p.pid)
            result[group] = pids

    return result


async def start(service: str = ""):
    """启动omega主进程或者任务管理进程

    Args:
        service: if service is '', then starts fetcher processes.

    Returns:

    """
    print(f"正在启动zillionare-omega {colored(service, 'green')}...")

    config_dir = get_config_dir()
    cfg4py.init(config_dir, False)

    if service == "":
        _start_fetcher_processes()
        await asyncio.sleep(5)
        _start_jobs()
    elif service == "jobs":
        return _start_jobs()
    elif service == "fetcher":
        return _start_fetcher_processes()
    else:
        print("不支持的服务")


def _start_fetcher_processes():
    procs = find_fetcher_processes()

    # fetcher processes are started by groups
    cfg = cfg4py.get_instance()
    for fetcher in cfg.quotes_fetchers:
        impl = fetcher.get("impl")
        workers = fetcher.get("workers")

        ports = [3181 + i for i in range(len(workers))]
        for group in workers:
            sessions = group.get("sessions")
            port = group.get("port") or ports.pop()
            account = group.get("account")
            password = group.get("password")
            started_sessions = procs.get(f"{impl}:{port}", [])
            if sessions - len(started_sessions) > 0:
                print(f"启动的{impl}实例少于配置要求（或尚未启动），正在启动中。。。")
                # sanic manages sessions, so we have to restart it as a whole
                for pid in started_sessions:
                    try:
                        os.kill(pid, signal.SIGTERM)
                    except Exception:
                        pass

                _start_fetcher(impl, account, password, port, sessions)
                time.sleep(1)

    time.sleep(3)
    show_fetcher_processes()


def show_fetcher_processes():
    print(f"正在运行中的omega-fetchers进程：\n{'=' * 40}")

    procs = find_fetcher_processes()

    if len(procs):
        print("   impl   |     port   |  pids")
        for group, pids in procs.items():
            impl, port = group.split(":")
            print(f"{impl:10}|{' ':5}{port:2}{' ':3}|  {pids}")
    else:
        print("None")


def _start_fetcher(
    impl: str, account: str, password: str, port: int, sessions: int = 1
):
    subprocess.Popen(
        [
            sys.executable,
            "-m",
            "omega.app",
            "start",
            f"--impl={impl}",
            f"--account={account}",
            f"--password={password}",
            f"--port={port}",
            f"--sessions={sessions}",
        ],
        stdout=subprocess.DEVNULL,
    )


def _start_jobs():
    subprocess.Popen(
        [
            sys.executable,
            "-m",
            "omega.jobs",
            "start",
            f"--port={cfg.omega.jobs.port}",
        ]
    )

    retry = 0
    while _find_jobs_process() is None and retry < 5:
        print("等待omega.jobs启动中")
        retry += 1
        time.sleep(1)
    if retry < 5:
        print("omega.jobs启动成功。")
    else:
        print("omega.jobs启动失败。")
        return

    _show_jobs_process()


def _restart_jobs():
    pid = _find_jobs_process()
    if pid is None:
        print("omega.jobs未运行。正在启动中...")
        _start_jobs()
    else:
        # 如果omega.jobs已经运行
        _stop_jobs()
        _start_jobs()


def _stop_jobs():
    pid = _find_jobs_process()
    retry = 0
    while pid is not None and retry < 5:
        retry += 1
        try:
            os.kill(pid, signal.SIGTERM)
            time.sleep(0.5)
        except Exception:
            pass
        pid = _find_jobs_process()

    if retry >= 5:
        print("未能停止omega.jobs")


def _show_jobs_process():
    print(f"正在运行中的jobs进程:\n{'=' * 40}")
    pid = _find_jobs_process()
    if pid:
        print(pid)
    else:
        print("None")


def _find_jobs_process():
    for p in psutil.process_iter():
        cmd = " ".join(p.cmdline())
        if cmd.find("omega.jobs") != -1:
            return p.pid
    return None


def _stop_fetcher_processes():
    retry = 0
    while retry < 5:
        procs = find_fetcher_processes()
        retry += 1
        if len(procs) == 0:
            return

        for group, pids in procs.items():
            for pid in pids:
                try:
                    os.kill(pid, signal.SIGTERM)
                except Exception:
                    pass
        time.sleep(1)

    if retry >= 5:
        print("未能终止fetcher进程")


async def status():
    show_fetcher_processes()
    print("\n")
    _show_jobs_process()


async def stop(service: str = ""):
    if service == "":
        _stop_jobs()
        _stop_fetcher_processes()
    elif service == "jobs":
        return _stop_jobs()
    else:
        _stop_fetcher_processes()


async def restart(service: str = ""):
    print("正在重启动服务...")
    await _init()

    if service == "":
        _stop_fetcher_processes()
        _stop_jobs()
        _start_fetcher_processes()
        _start_jobs()
    elif service == "jobs":
        return _restart_jobs()
    else:
        _stop_fetcher_processes()
        _start_fetcher_processes()


async def sync_sec_list():
    """发起同步证券列表请求"""
    await _init()

    await syncjobs.trigger_single_worker_sync("security_list")


async def sync_calendar():
    """发起同步交易日历请求"""
    await _init()
    await syncjobs.trigger_single_worker_sync("calendar")


async def sync_bars(frame: str = None, codes: str = None):
    """立即同步行情数据

    如果`frame`, `codes`没有提供，则从配置文件中读取相关信息

    Args:
        frame:
        codes:

    Returns:

    """
    await _init()

    if frame:
        frame_type = FrameType(frame)
        params = syncjobs.load_sync_params(frame_type)
        if codes:
            params["cat"] = None
            params["include"] = codes
        await syncjobs.trigger_bars_sync(params, force=True)
        logger.info("request %s,%s send to workers.", params, codes)
    else:
        for frame_type in itertools.chain(tf.day_level_frames, tf.minute_level_frames):
            params = syncjobs.load_sync_params(frame_type)
            if not params:
                continue
            if codes:
                params["cat"] = None
                params["include"] = codes
            await syncjobs.trigger_bars_sync(params, force=True)

            logger.info("request %s,%s send to workers.", params, codes)


async def http_get(url, content_type: str = "json"):
    try:
        async with aiohttp.ClientSession() as client:
            async with client.get(url) as resp:
                if resp.status == 200:
                    if content_type == "json":
                        return await resp.json()
                    elif content_type == "text":
                        return await resp.text()
                    else:
                        return await resp.content.read(-1)
    except Exception as e:
        logger.exception(e)

    return None


async def get_archive_index():
    url = cfg.omega.urls.archive + f"/index.yml?{random.random()}"
    content = await http_get(url, "text")
    if content is None:
        print("当前没有历史数据可供下载")
        return

    return archive.parse_index(content)


def bin_cut(arr: list, n: int):
    """将数组arr切分成n份

    Args:
        arr ([type]): [description]
        n ([type]): [description]

    Returns:
        [type]: [description]
    """
    result = [[] for i in range(n)]

    for i, e in enumerate(arr):
        result[i % n].append(e)

    return [e for e in result if len(e)]


async def show_subprocess_output(stream):
    while True:
        try:
            line = await stream.readline()
            line = line.decode("utf-8")
            if not line:
                break

            # this is logger output
            if line.find(" I ") != -1:
                continue
            print(line)
        except Exception:
            pass


async def download_archive(n: Union[str, int] = None):
    index = await get_archive_index()

    avail_months = [int(m) for m in index.get("stock")]
    avail_months.sort()
    if avail_months is None:
        print("当前没有历史数据可供下载")
        return
    else:
        prompt = f"现有截止到{avail_months[-1]}的{len(avail_months)}个月的数据可供下载。"

    if n is None:
        op_hint = "请输入要下载的数据的月数，0表示不下载:"

        def is_valid(x):
            try:
                return 0 < int(x) <= len(avail_months)
            except Exception:
                return False

        n = int(get_input(prompt, is_valid, None, op_hint=op_hint))
    else:
        n = int(n)
    if n is None or n <= 0:
        return

    t0 = time.time()
    n = min(n, len(avail_months))
    # months = ",".join([str(x) for x in avail_months[-n:]])
    cats = "stock"

    cpus = psutil.cpu_count()

    months_groups = bin_cut(avail_months[-n:], cpus)
    tasks = []
    print(f"共启动{len(months_groups)}个进程，正在下载中...")
    for m in months_groups:
        if len(m) == 0:
            break
        months = ",".join([str(x) for x in m])

        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "omega.fetcher.archive",
            "main",
            f"'{months}'",
            f"'{cats}'",
            cfg.omega.urls.archive,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        tasks.append(show_subprocess_output(proc.stdout))
        tasks.append(show_subprocess_output(proc.stderr))

    await asyncio.gather(*tasks)
    print(f"数据导入共费时{int(time.time() - t0)}秒")


def remove_console_log_handler():
    root_logger = logging.getLogger()
    for h in root_logger.handlers:
        if isinstance(h, logging.StreamHandler):
            root_logger.removeHandler(h)


async def _init():
    config_dir = get_config_dir()
    cfg = cfg4py.init(config_dir, False)

    # remove console log, so the output message will looks prettier
    remove_console_log_handler()
    try:
        await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)
    except Exception:
        print(f"dsn is {cfg.redis.dsn}")

    impl = cfg.quotes_fetchers[0]["impl"]
    params = cfg.quotes_fetchers[0]["workers"][0]
    await AbstractQuotesFetcher.create_instance(impl, **params)

    await omicron.init(AbstractQuotesFetcher)


def run_with_init(func):
    def wrapper(*args, **kwargs):
        async def init_and_run(*args, **kwargs):
            try:
                await _init()
                # os.system("clear")
                await func(*args, **kwargs)
            except CancelError:
                pass
            finally:
                await omicron.cache.close()

        asyncio.run(init_and_run(*args, **kwargs))

    return wrapper


def run(func):
    def wrapper(*args, **kwargs):
        asyncio.run(func(*args, **kwargs))

    return wrapper


def main():
    import warnings

    warnings.simplefilter("ignore")

    fire.Fire(
        {
            "start": run(start),
            "setup": run(setup),
            "stop": run(stop),
            "status": run(status),
            "restart": run(restart),
            "sync_sec_list": run_with_init(sync_sec_list),
            "sync_calendar": run_with_init(sync_calendar),
            "sync_bars": run_with_init(sync_bars),
            "download": run_with_init(download_archive),
        }
    )


if __name__ == "__main__":
    main()
