import asyncio
import logging
import os

import cfg4py
import omicron
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from omicron.models.stock import Security
from omicron.models.timeframe import TimeFrame
from sanic import Sanic

from omega.boards.board import ConceptBoard, IndustryBoard
from omega.config import get_config_dir
from omega.webservice.stockinfo import GlobalStockInfo
from omega.webservice.web_bp import init_web_api_blueprint

logger = logging.getLogger(__name__)

app = Sanic("trade-server-webservice")


async def reload_calendar():
    await TimeFrame.init()
    logger.info("reload_calendar success.")


async def reload_securities():
    await Security.load_securities()
    await GlobalStockInfo.load_all_securities()
    logger.info("reload_securities success.")


def load_cron_tasks(scheduler):
    scheduler.add_job(
        reload_calendar,
        "cron",
        hour=1,
        minute=12,
        second=5,
        name="reload_calendar",
    )

    scheduler.add_job(
        reload_securities,
        "cron",
        hour=8,
        minute=12,
        second=5,
        name="reload_securities",
    )


def start_scheduler():
    """启动定时任务"""
    cfg = cfg4py.get_instance()

    scheduler = AsyncIOScheduler(timezone=cfg.tz)
    load_cron_tasks(scheduler)
    scheduler.start()


async def server_init():
    try:
        await omicron.init()
    except Exception as e:
        print(
            "init failed, make sure you have calendar and securities data in store: %s",
            str(e),
        )
        await asyncio.sleep(5)
        os._exit(1)


async def initialize_server(app, loop):
    await server_init()

    await GlobalStockInfo.load_all_securities()

    start_scheduler()

    IndustryBoard.init()
    ConceptBoard.init()


def set_initialize_start():
    init_web_api_blueprint(app)
    app.before_server_start(initialize_server)


def start_sanic_server():
    cfg4py.init(get_config_dir(), False)
    cfg = cfg4py.get_instance()
    port = cfg.omega.http_port  # 3180

    set_initialize_start()

    app.run(host="0.0.0.0", port=port, workers=1, single_process=True)


if __name__ == "__main__":
    start_sanic_server()
