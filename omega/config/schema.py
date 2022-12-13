# noqa
from typing import Optional


class Config(object):
    __access_counter__ = 0

    def __cfg4py_reset_access_counter__(self):
        self.__access_counter__ = 0

    def __getattribute__(self, name):
        obj = object.__getattribute__(self, name)
        if name.startswith("__") and name.endswith("__"):
            return obj

        if callable(obj):
            return obj

        self.__access_counter__ += 1
        return obj

    def __init__(self):
        raise TypeError("Do NOT instantiate this class")

    tz: Optional[str] = None

    class logreceiver:
        klass: Optional[str] = None

        dsn: Optional[str] = None

        channel: Optional[str] = None

        filename: Optional[str] = None

        backup_count: Optional[int] = None

        max_bytes: Optional[str] = None

    class redis:
        dsn: Optional[str] = None

    class pickle:
        ver: Optional[int] = None

    class omega:
        http_port: Optional[int] = None

    quotes_fetchers: Optional[list] = None

    class dfs:
        engine: Optional[str] = None

        class minio:
            host: Optional[str] = None

            port: Optional[int] = None

            access: Optional[str] = None

            secret: Optional[str] = None

            secure: Optional[bool] = None

            bucket: Optional[str] = None

    class zarr:
        store_path: Optional[str] = None

    class influxdb:
        url: Optional[str] = None

        token: Optional[str] = None

        org: Optional[str] = None

        bucket_name: Optional[str] = None

        enable_compress: Optional[bool] = None

        max_query_size: Optional[int] = None

    class notify:
        mail_from: Optional[str] = None

        mail_to: Optional[list] = None

        mail_server: Optional[str] = None

        dingtalk_access_token: Optional[str] = None

        dingtalk_secret: Optional[str] = None
