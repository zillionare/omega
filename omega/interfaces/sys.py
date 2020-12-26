from sanic import response

from omega import __version__
from sanic import Blueprint

bp = Blueprint("sys", url_prefix="/sys/")


@bp.route("version")
async def get_version(request):
    return response.text(__version__)
