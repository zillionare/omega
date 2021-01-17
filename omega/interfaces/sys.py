from sanic import Blueprint, response

from omega import __version__

bp = Blueprint("sys", url_prefix="/sys/")


@bp.route("version")
async def get_version(request):
    return response.text(__version__)
