import logging
import pickle

import arrow
import cfg4py
from omicron.core.timeframe import tf
from omicron.core.types import FrameType
from sanic import Blueprint, response

from omega.fetcher.abstract_quotes_fetcher import AbstractQuotesFetcher as aq

bp = Blueprint("quotes", url_prefix="/quotes/")

logger = logging.getLogger(__name__)
cfg = cfg4py.get_instance()


@bp.route("valuation")
async def get_valuation(request):
    try:
        secs = request.json.get("secs")
        date = arrow.get(request.json.get("date")).date()
        fields = request.json.get("fields")
        n = request.json.get("n", 1)
    except Exception as e:
        logger.exception(e)
        logger.error("problem params:%s", request.json)
        return response.empty(status=400)
    try:
        valuation = await aq.get_valuation(secs, date, fields, n)
        body = pickle.dumps(valuation, protocol=cfg.pickle.ver)
        return response.raw(body)
    except Exception as e:
        logger.exception(e)
        return response.raw(pickle.dumps(None, protocol=cfg.pickle.ver))


@bp.route("security_list")
async def get_security_list_handler(request):
    secs = await aq.get_security_list()

    body = pickle.dumps(secs, protocol=cfg.pickle.ver)
    return response.raw(body)


@bp.route("bars_batch")
async def get_bars_batch_handler(request):
    try:
        secs = request.json.get("secs")
        frame_type = FrameType(request.json.get("frame_type"))

        end = arrow.get(request.json.get("end"), tzinfo=cfg.tz)
        end = end.date() if frame_type in tf.day_level_frames else end.datetime

        n_bars = request.json.get("n_bars")
        include_unclosed = request.json.get("include_unclosed", False)

        bars = await aq.get_bars_batch(secs, end, n_bars, frame_type, include_unclosed)

        body = pickle.dumps(bars, protocol=cfg.pickle.ver)
        return response.raw(body)
    except Exception as e:
        logger.exception(e)
        return response.raw(pickle.dumps(None, protocol=cfg.pickle.ver))


@bp.route("bars")
async def get_bars_handler(request):
    try:
        sec = request.json.get("sec")
        frame_type = FrameType(request.json.get("frame_type"))

        end = arrow.get(request.json.get("end"), tzinfo=cfg.tz)
        end = end.date() if frame_type in tf.day_level_frames else end.datetime
        n_bars = request.json.get("n_bars")
        include_unclosed = request.json.get("include_unclosed", False)

        bars = await aq.get_bars(sec, end, n_bars, frame_type, include_unclosed)

        body = pickle.dumps(bars, protocol=cfg.pickle.ver)
        return response.raw(body)
    except Exception as e:
        logger.exception(e)
        return response.raw(pickle.dumps(None, protocol=cfg.pickle.ver))


@bp.route("all_trade_days")
async def get_all_trade_days_handler(request):
    days = await aq.get_all_trade_days()

    body = pickle.dumps(days, protocol=cfg.pickle.ver)
    return response.raw(body)
