class Events:
    SECURITY_LIST_UPDATED = "quotes/security_list_updated"
    OMEGA_WORKER_JOIN = "omega/worker_join"
    OMEGA_WORKER_LEAVE = "omega/worker_leave"
    OMEGA_WORKER_HEARTBEAT = "omega/worker_heartbeat"

    OMEGA_APP_START = "omega/app_start"
    OMEGA_APP_STOP = "omega/app_stop"

    OMEGA_DO_SYNC = "omega/sync_bars_worker"
    OMEGA_VALIDATION_PROGRESS = "omega/do_validation"
    OMEGA_DO_CHECKSUM = "omega/do_checksum"
    OMEGA_VALIDATION_ERROR = "omega/validation_error"

    OMEGA_DO_SYNC_MIN = "omega/sync_min"
    OMEGA_DO_SYNC_DAY = "omega/sync_day"
    OMEGA_DO_SYNC_DAILY_CALIBRATION = "omega/daily_calibration"
    OMEGA_DO_SYNC_TRADE_PRICE_LIMITS = "omega/trade_price_limits"
    # year_quarter_month_week
    OMEGA_DO_SYNC_YEAR_QUARTER_MONTH_WEEK = "omega/year_quarter_month_week"
    OMEGA_DO_SYNC_OTHER_MIN = "omega/min_5_15_30_60"
    OMEGA_DO_SYNC_SECURITIES = "omega/sycn_securites"
    OMEGA_DO_SYNC_XRXD_REPORTS = "omega/sycn_xrxd_reports"

    OMEGA_HEART_BEAT = "omega/heart_beat"


class ValidationError:
    NO_CHECKSUM = 0
    REMOTE_MISS = 1
    LOCAL_MISS = 2
    MISMATCH = 3
    UNKNOWN = 4
