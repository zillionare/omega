import arrow
from omicron.models.stock import Stock
from omega.master.tasks.synctask import BarsSyncTask
from coretypes import FrameType, SecurityType
from omega.core.events import Events
from omega.core import constants
from omicron.dal import cache
import omega.master.jobs as syncjobs
from omicron import init
import cfg4py
from pyemit import emit
from unittest import mock

@mock.patch(
        "omega.master.tasks.synctask.QuotaMgmt.check_quota",
        return_value=((True, 1000000, 500000)),
    )
async def sync_minute_bars(dummy):
    name = "minute"
    timeout = 60
    end = arrow.now()

    task = BarsSyncTask(
        event=Events.OMEGA_DO_SYNC_MIN,
        name=name,
        frame_type=[FrameType.MIN1],
        end=end.naive,
        n_bars=1,
        timeout=timeout,
    )

    await task.cleanup()
    await task.run()

async def main():
    cfg = cfg4py.init("/root/zillionare/omega/config")
    await init()
    await emit.start(emit.Engine.REDIS, dsn=cfg.redis.dsn)
    await sync_minute_bars()

import asyncio
asyncio.run(main())
 