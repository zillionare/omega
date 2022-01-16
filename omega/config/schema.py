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

    class postgres:
        dsn: Optional[str] = None

        enabled: Optional[bool] = None

    class pickle:
        ver: Optional[int] = None

    class omega:
        home: Optional[str] = None

        class jobs:
            port: Optional[int] = None

        class urls:
            quotes_server: Optional[str] = None

            archive: Optional[str] = None

        heartbeat: Optional[int] = None

        class sync:
            security_list: Optional[str] = None

            calendar: Optional[str] = None

            class bars:
                cat: Optional[list] = None

    quotes_fetchers: Optional[list] = None

    class dfs:
        engine: Optional[str] = None

        class minio:
            host: Optional[str] = None

            port: Optional[str] = None

            access: Optional[str] = None

            secret: Optional[str] = None

            secure: Optional[bool] = None

            bucket: Optional[str] = None

    class influxdb:
        url: Optional[str] = None

        token: Optional[str] = None

        org: Optional[str] = None

        bucket_name: Optional[str] = None

    class notify:
        mail_from: Optional[str] = None

        mail_to: Optional[list] = None

        mail_server: Optional[str] = None
