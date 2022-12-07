import logging

import arrow
from sanic import Blueprint, Sanic, response

from omega.boards.webapi import (
    board_filter_members,
    board_fuzzy_match,
    concepts_info_by_sec,
    get_board_bars_bycount,
    get_board_info_by_id,
    get_boards_by_sec,
    industry_info_by_sec,
    list_boards,
)
from omega.webservice.stockinfo import frame_count, frame_shift, get_stock_info

logger = logging.getLogger(__name__)

bp_webapi = Blueprint("web_api_blueprint", url_prefix="/api", strict_slashes=False)


@bp_webapi.route("/timeframe/frame_shift", methods=["POST"])
async def bp_webapi_frame_shift(request):
    dt_str = request.json.get("dt", None)
    if not dt_str:
        return response.json({})
    _tmp_dt = arrow.get(dt_str).naive

    ft_str = request.json.get("ft", None)
    if not ft_str:
        return response.json({})

    _str = request.json.get("count", None)
    n_count = int(_str)

    rc = await frame_shift(_tmp_dt, ft_str, n_count)
    return response.json(rc)


@bp_webapi.route("/timeframe/frame_count", methods=["POST"])
async def bp_webapi_frame_count(request):
    dt1_str = request.json.get("start", None)
    dt2_str = request.json.get("end", None)
    if not dt1_str or not dt2_str:
        return response.json({})

    _tmp_dt1 = arrow.get(dt1_str).naive
    _tmp_dt2 = arrow.get(dt2_str).naive

    ft_str = request.json.get("ft", None)
    if not ft_str:
        return response.json({})

    rc = await frame_count(_tmp_dt1, _tmp_dt2, ft_str)
    return response.json(rc)


@bp_webapi.route("/stock/info", methods=["POST"])
async def bp_admin_stock_info(request):
    security = request.json.get("security")
    if not security:
        return response.json({})
    rc = await get_stock_info(security)
    return response.json(rc)


@bp_webapi.route("/board/board_list", methods=["POST"])
async def bp_webapi_board_list(request):
    """获取全部板块信息：名称，代码

    返回值：
        [('309020', '信创', 10)]
    """

    board_type = request.json.get("board_type", None)
    if not board_type:
        return response.json([])

    rc = list_boards(board_type)
    return response.json(rc)


@bp_webapi.route("/board/industry_list_by_sec", methods=["POST"])
async def bp_webapi_ib_list_by_sec(request):
    security = request.json.get("security", None)
    if not security:
        return response.json({})

    rc = industry_info_by_sec(security)
    return response.json(rc)


@bp_webapi.route("/board/concept_list_by_sec", methods=["POST"])
async def bp_webapi_cb_list_by_sec(request):
    security = request.json.get("security", None)
    if not security:
        return response.json({})

    rc = concepts_info_by_sec(security)
    return response.json(rc)


@bp_webapi.route("/board/get_name", methods=["POST"])
async def bp_webapi_board_get_name(request):
    """模糊匹配板块或概念名称（中文）

    返回值：
        ['308935 方舱医院', '308893 毛发医疗', '308885 家庭医生', '308733 牙科医疗']
    """

    board_type = request.json.get("board_type")
    pattern = request.json.get("pattern")
    if not pattern or not board_type:
        return response.json([])

    rc = board_fuzzy_match(board_type, pattern)
    return response.json(rc)


@bp_webapi.route("/board/info", methods=["POST"])
async def bp_webapi_board_get_info(request):
    """获取板块信息：名称，代码，股票成员数目

    返回值：
        {'code': '301505', 'name': '医疗器械概念', 'stocks': 242}
        or
        {'code': '301505', 'name': '医疗器械概念', 'stocks': [['300916', '朗特智能'], ['300760', '迈瑞医疗']]}
    """

    board_type = request.json.get("board_type")
    board_id = request.json.get("board_id")
    _mode = request.json.get("fullmode", None)
    if not board_type or not board_id:
        return response.json({})

    if _mode is None:
        _mode = 0
    rc = get_board_info_by_id(board_type, board_id, _mode)
    return response.json(rc)


@bp_webapi.route("/board/info_by_sec", methods=["POST"])
async def bp_webapi_board_info_by_sec(request):
    """获取股票所在板块信息：名称，代码

    返回值：
        [{'code': '301505', 'name': '医疗器械概念'}]
    """

    board_type = request.json.get("board_type")
    security = request.json.get("security")
    if not board_type or not security:
        return response.json([])

    sec_code = security.split(".")[0]
    rc = get_boards_by_sec(board_type, sec_code)
    return response.json(rc)


@bp_webapi.route("/board/board_filter_members", methods=["POST"])
async def bp_webapi_board_filter_members(request):
    """根据板块名筛选股票，参数为include, exclude

    返回值：
        [['300181', '佐力药业'], ['600056', '中国医药']]
    """

    board_type = request.json.get("board_type")
    _included = request.json.get("include_boards")
    _excluded = request.json.get("exclude_boards")
    if not board_type or not _included:
        return response.json([])

    if not isinstance(_included, list):
        return response.json([])
    if _excluded:
        if not isinstance(_excluded, list):
            return response.json([])
    else:
        _excluded = []

    rc = board_filter_members(board_type, _included, _excluded)
    return response.json(rc)


@bp_webapi.route("/board/board_bars_by_count", methods=["POST"])
async def bp_webapi_board_bars_info(request):
    """获取板块详细信息，K线图和MA数组

    Args:
        code (str): 股票代码
        dt_end (datetime.datetime): 结束时间，闭区间
        n_bars (int): bars的数目

    Returns:
        {
            "bars": [(open,high,low,close,volume)],
            "ma5": [],
            "ma10": [],
            "ma20": [],
        }
    """

    code = request.json.get("board_id", None)
    _end = request.json.get("end", None)
    n_bars = request.json.get("n_bars", None)
    if not code or not _end or not n_bars:
        return response.json({})

    dt_end = arrow.get(_end).naive.date()
    if n_bars > 250:
        n_bars = 250
    if n_bars < 5:
        n_bars = 5

    _code = f"{code}.THS"
    rc = await get_board_bars_bycount(_code, dt_end, n_bars)
    return response.json(rc)


def init_web_api_blueprint(app: Sanic):
    app.blueprint(bp_webapi)
    logger.info("WebAPI blueprint added into app object")
