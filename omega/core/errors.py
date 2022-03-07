class Error(Exception):
    """错误基类"""

    def __init__(self, message: str, *args):
        self.message = message
        self.args = args

    def __str__(self):
        return f"{self.message}: {self.args}"


class QuotaExceededError(Error):
    """配额超限错误"""

    pass
