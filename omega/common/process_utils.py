import psutil


def find_jobs_process():
    for p in psutil.process_iter():
        try:
            cmd = " ".join(p.cmdline())
            if cmd.find("omega.master") != -1:
                return p.pid
        except (PermissionError, ProcessLookupError):
            pass
    return None
