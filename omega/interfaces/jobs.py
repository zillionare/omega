import logging

from sanic import response
from sanic.blueprints import Blueprint

from omega.jobs import syncjobs

bp = Blueprint("jobs", url_prefix="/jobs/")
logger = logging.getLogger(__name__)


@bp.route("sync_calendar", methods=["POST"])
async def sync_calendar_handler(request):
    try:
        await syncjobs.sync_calendar()
        return response.json(body=None, status=200)
    except Exception as e:
        logger.exception(e)
        return response.json(e, status=500)


@bp.route("sync_security_list", methods=["POST"])
async def sync_seurity_list_handler(request):
    try:
        await syncjobs.sync_security_list()
        return response.json(body=None, status=200)
    except Exception as e:
        logger.exception(e)
        return response.json(body=e, status=500)


@bp.route("sync_bars", methods=["POST"])
async def bars_sync_handler(request):
    if request.json:
        secs = request.json.get("secs")
        frames_to_sync = request.json.get("frames_to_sync")
    else:
        secs = None
        frames_to_sync = None

    app = request.app
    app.add_task(syncjobs.trigger_bars_sync(secs, frames_to_sync))
    return response.text(f"sync_bars with {secs}, {frames_to_sync} is scheduled.")
