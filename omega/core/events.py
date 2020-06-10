class Events:
    SECURITY_LIST_UPDATED = "quotes/security_list_updated"
    OMEGA_WORKER_JOIN = "omega/worker_join"
    OMEGA_WORKER_LEAVE = "omega/worker_leave"
    OMEGA_WORKER_HEARTBEAT = "omega/worker_heartbeat"

    OMEGA_APP_START = "omega/app_start"
    OMEGA_APP_STOP = "omega/app_stop"

    OMEGA_DO_SYNC = "omega/do_sync"
    OMEGA_VALIDATION_PROGRESS = "omega/do_validation"
    OMEGA_DO_CHECKSUM = "omega/do_checksum"
    OMEGA_VALIDATION_ERROR = "omega/validation_error"
