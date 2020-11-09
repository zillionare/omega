#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Author: Aaron-Yang [code@jieyu.ai]
Contributors:

"""
import asyncio
import json
import logging

import arrow
import jqdatasdk as jq
import numpy as np
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyemit import emit
from termcolor import colored

logger = logging.getLogger(__name__)


def ewm_vectorized(data, win, order="C"):
    """
    Calculates the exponential moving average over a direction.
    Will fail for large inputs.
    :param data: Input data
    :param win: scalar int
        The window parameter for the moving average.
    :param offset: optional
        The offset for the moving average, scalar. Defaults to data[0].
    :param dtype: optional
        Data type used for calculations. Defaults to float64 unless
        data.dtype is float32, then it will use float32.
    :param order: {'C', 'F', 'A'}, optional
        Order to use when flattening the data. Defaults to 'C'.
    """
    sum_proportion = 0.99
    alpha = 1 - np.exp(np.log(1 - sum_proportion) / win)
    dtype = data.dtype

    if data.ndim > 1:
        # flatten input
        data = data.reshape(-1, order)

    out = np.empty_like(data, dtype=dtype)

    if data.size < 1:
        # empty input, return empty array
        return out

    offset = data[0]

    alpha = np.array(alpha, copy=False).astype(dtype, copy=False)

    # scaling_factors -> 0 as len(data) gets large
    # this leads to divide-by-zeros below
    scaling_factors = np.power(
        1.0 - alpha, np.arange(data.size + 1, dtype=dtype), dtype=dtype
    )
    # create cumulative sum array
    np.multiply(
        data, (alpha * scaling_factors[-2]) / scaling_factors[:-1], dtype=dtype, out=out
    )
    np.cumsum(out, dtype=dtype, out=out)

    # cumsums / scaling
    out /= scaling_factors[-2::-1]

    if offset != 0:
        offset = np.array(offset, copy=False).astype(dtype, copy=False)
        # add offsets
        out += offset * scaling_factors[1:]

    return out


def macd(data: np.array, short: int, long: int, m: int):
    diff = ewm_vectorized(data, short) - ewm_vectorized(data, long)
    dea = ewm_vectorized(data, m)
    macd = 2 * (diff - dea)

    return diff, dea, macd


def report(msg: str, color: str = None):
    now = arrow.now()
    print(colored(f"{now.hour:02}:{now.minute:02} {msg}", color))


class Security:
    dtype = [
        ("frame", "O"),
        ("open", "f4"),
        ("high", "f4"),
        ("low", "f4"),
        ("close", "f4"),
        ("volume", "f8"),
        ("amount", "f8"),
    ]

    def __init__(self, code: str, signal: int = 0):
        self.code = code
        self.day_bars = None
        self.m30_bars = None

        # 均价线
        self.day_pma = None
        self.m30_pma = None

        # ma均线
        self.day_ma = {}
        self.m30_ma = {}

        # 0 signal on both long and short; 1 signal on long; -1 signal on short
        self.signal = signal

    def update_indicators(self, frame_type: str):
        if frame_type == "1d":
            self.day_pma = self.day_bars["money"] / self.day_bars["volume"]
            self.ma(frame_type)
        elif frame_type == "30m":
            self.m30_pma = self.m30_bars["money"] / self.m30_bars["volume"]
            self.ma(frame_type)

    def ma(self, frame_type):
        if frame_type == "1d":
            arr = self.day_ma
        elif frame_type == "30m":
            arr = self.m30_ma
        else:
            raise ValueError(f"unsupported frame_type: {frame_type}")

        for win in [5, 10, 20, 60, 120, 250]:
            name = f"ma{win}"
            arr[name] = np.convolve(arr, np.ones(win), "valid") / win

    # def macd(self, price: np.array):
    #     dif, dea, macd=talib.MACD(price, fastperiod=12, slowperiod=26, signalperiod=9)
    #     macd = macd * 2

    #     return dif, dea, macd

    # def ppo(self, price: np.array):
    #     return talib.PPO(price)

    def mom(self, price: np.array, decimals=4):
        """
        股价动量（涨速），为股价的一阶导。
        返回结果为数组，其长度为len(price) - 3
        Args:
            price:

        Returns:

        """
        assert len(price) > 3
        # 标准化为涨幅
        ts = price[1:] / price[:-1] - 1
        # 先进行平滑，再求二阶导，最后返回win周期以内二阶导平均值
        # todo recover me
        # kernel = np.array([0.6, 0.3, 0.1])  # cause length -= 2
        # ts = np.convolve(ts, kernel, 'valid')  # kernel is reverse direction

        return np.array([round(x, decimals) for x in ts])

    def merge_bars(self, bars: np.array, frame_type: str) -> np.array:
        if frame_type == "1d":
            if self.day_bars is None:
                self.day_bars = bars
            else:
                assert len(bars) == 2
                self.day_bars = self._merge_bars(self.day_bars, bars)

            self.update_indicators(frame_type)
            return self.day_bars

        if frame_type == "30m":
            if self.m30_bars is None:
                self.m30_bars = bars
            else:
                self.m30_bars = self._merge_bars(self.m30_bars, bars)

            self.update_indicators(frame_type)
            return self.m30_bars

    def _merge_bars(self, old: np.array, new: np.array):
        """将新取得的两根bars合并到old所指向的已有数据上。 可能存在：

        1. 新数据与旧数据最后两frame完全一致
        2. 新数据与旧数据连续，但更新了最后一个frame
        3. 新数据与旧数据连续，但增加了一个新的frame
        4. 不连续，程序错误
        Args:
            old:
            new:

        Returns:

        """
        assert len(new) == 2

        n0, n1 = new[0], new[1]
        o0, o1 = old[-2], old[-1]
        if n0["date"] == o0["date"] and n1["date"] == o1["date"]:
            logger.debug(
                "重复数据: (%s,%s), (%s,%s)", n0["date"], o0["date"], n1["date"], o1["date"]
            )
        elif o0["date"] == n0["date"] and o1["date"] < n1["date"]:
            old[-1] = n1
            old = np.append(old[:-1], n1)
        elif o1["date"] == n0["date"]:
            old = np.append(old, n1)[1:]
        else:
            logger.warning("数据不连续. %s, %s", self.m30_bars[-1]["date"], new[0]["date"])

        return old

    def macd_deviation(self, macd: np.array):
        pass

    async def evaluate(self):
        direct, std_err, ddts = self.direction(self.m30_pma[-10:])

        # 转换为百分数
        direct *= 100
        std_err *= 100
        ddts *= 100
        mom = self.mom(self.m30_pma[-10:]) * 100

        report(f"涨速({mom[-1]}):{mom[-4]} {mom[-3]} {mom[-2]} {mom[-1]}")

        if direct > 0:
            report(f"向上变盘中({direct} {std_err} {ddts}", "red")
        elif direct < 0:
            report(f"向下变盘中({direct} {std_err} {ddts}", "green")

        diff, dea, macd = self.macd(self.m30_pma[-10:])
        macd_direction = self.direction(macd[-5:])
        report(f"macd driection:{macd_direction}, macd:{macd}")

        # if direct > 0:
        #     report(f"macd向上变化中({direct} {std_err} {ddts}", "red")
        # elif direct < 0:
        #     report(f"macd向下变化中({direct} {std_err} {ddts}", "green")


class OnePercent:
    fields = ["date", "open", "high", "low", "close", "volume", "money"]

    def __init__(self):
        self.secs = {}
        self.scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")

    async def load_sec_list(self, file: str):
        with open(f"./{file}", "r") as f:
            secs = json.load(f)

        copy = {}
        for code in secs["long"]:
            if code in self.secs:
                sec = self.get_security(code)
                sec.signal = 1
            else:
                copy[code] = Security(code, 1)

        for code in secs["short"]:
            if code in self.secs:
                sec = self.get_security(code)
                sec.signal = -1
            else:
                copy[code] = Security(code, -1)

        for code in secs["both"]:
            if code in self.secs:
                sec = self.get_security(code)
                sec.signal = 0
            else:
                copy[code] = Security(code, 0)

        self.secs = copy

        for code in self.secs.keys():
            await self.get_bars(code, "1d", include_unclosed=False)
            await self.get_bars(code, "30m", include_unclosed=True)

        await self.reset_jobs()
        logger.info("monitoring %s secs", len(self.secs))

    def get_security(self, sec: str) -> Security:
        return self.secs[sec]

    async def reset_jobs(self):
        self.scheduler.remove_all_jobs()
        for sec in self.secs.keys():
            self.scheduler.add_job(
                self.get_bars, trigger="cron", minute="*/3", args=[sec, "30m"]
            )

    async def get_bars(self, code: str, frame_type: str, include_unclosed=True):
        # 如果非交易时间，则返回空
        # 如果sec.day_bars 未空，则请求260根k线
        now = arrow.now("Asia/Shanghai")
        minutes = now.datetime.hour * 60 + now.datetime.minute
        if not (690 > minutes > 570 or 780 < minutes < 900):
            logger.debug("not in trade time")
            # return

        sec = self.get_security(code)

        if frame_type == "1d":
            n = 260 if sec.day_bars is None else 2
        elif frame_type == "30m":
            n = 260 if sec.m30_bars is None else 2
        else:
            raise ValueError(f"{frame_type} not supported")

        logger.info("request %s, %s, %s", code, frame_type, n)
        bars = jq.get_bars(
            code,
            n,
            frame_type,
            self.fields,
            include_now=include_unclosed,
            df=False,
            fq_ref_date=arrow.now().date(),
        )
        await emit.emit(
            "/new_bars_data", {"code": code, "frame_type": frame_type, "bars": bars}
        )

    async def on_new_data(self, msg):
        code = msg.get("code")
        frame_type = msg.get("frame_type")
        bars = msg.get("bars")

        logger.info(
            "new bars arrived: %s, %s, %s, %s",
            code,
            frame_type,
            bars[-1]["date"],
            len(bars),
        )

        sec = self.get_security(code)
        if sec is None:
            logger.warning("wrong code: %s", code)

        # merge bars
        sec.merge_bars(bars, frame_type)
        if frame_type == "30m":
            await sec.evaluate()

    async def start(self):
        emit.register("/new_bars_data", self.on_new_data)
        await emit.start(emit.Engine.IN_PROCESS)
        await self.load_sec_list("secs.json")
        self.scheduler.start()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    pyemit_logger = logging.getLogger("pyemit")
    pyemit_logger.setLevel(logging.WARNING)
    #
    # apscheduler_logger = logging.getLogger('apscheduler')
    # apscheduler_logger.setLevel(logging.WARNING)

    one = OnePercent()
    loop = asyncio.get_event_loop()
    loop.create_task(one.start())
    loop.run_forever()
