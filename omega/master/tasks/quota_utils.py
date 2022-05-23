import logging
from threading import Lock
from typing import Tuple

import arrow

logger = logging.getLogger(__name__)


class QuotaMgmt:
    work_state = {}
    # work的状态,{'account': {'account': xxxx, 'quota': xxx, 'impl': 'jq', 'time': '2022-05-11 15:20:29'}}
    quota_stat_q1 = 0  # 当日剩余配额，交易时间段以外的全部任务
    quota_stat_q2 = 0  # 当日剩余配额，白天的盘中任务
    quota_date = None
    quota_lock = Lock()  # 读写上面的quota_stat时，需要加锁

    @classmethod
    def update_state(cls, params: dict):
        # hearbeat定时更新quota数据
        account = params.get("account")

        # 当前无法事先分配worker，因此不支持多个数据服务商
        # impl = params.get("impl")
        # key = f"{impl}:{account}"

        try:
            cls.quota_lock.acquire()
            cls.work_state[account] = params
        finally:
            cls.quota_lock.release()

    @classmethod
    def get_quota(cls):
        """获取quota的数量，需要根据worker对应的impl里面的账号计算，目前暂时支持单个impl进程"""
        quota = 0
        total = 0
        for worker in cls.work_state.values():
            quota = worker.get("quota", 0)
            total = worker.get("total", 0)
            break  # 当前只会有一个worker

        return (quota, total)

    @classmethod
    def update_quota(cls):
        today = arrow.now().naive.date()

        # 获取最新的quota信息
        quota, total = cls.get_quota()
        if cls.quota_date is None or cls.quota_date != today:
            cls.quota_date = today  # 初始化当天的配额
            reserved = int(total * 0.25)  # 保留25%给白天交易时段同步使用
            if reserved > quota:
                cls.quota_stat_q1 = 0
                cls.quota_stat_q2 = quota
            else:
                cls.quota_stat_q1 = quota - reserved
                cls.quota_stat_q2 = reserved
        else:
            q1 = cls.quota_stat_q1
            q2 = cls.quota_stat_q2
            if quota < q1 + q2:  # 有其他任务占用了quota，需要重新计算
                delta = (q1 + q2) - quota
                q1 -= delta  # 扣除到非交易时段的配额上
                if q1 < 0:  # 不够扣除，则扣除到交易时段的配额上
                    q2 = quota  # 配额全部保留给交易时段的同步任务
                    q1 = 0
                cls.quota_stat_q1 = q1
                cls.quota_stat_q2 = q2

    @classmethod
    def check_quota(cls, quota_type, count) -> Tuple:
        """检查quota是否足够完成本次同步
        Returns:
            返回Tuple(isok, spare quota, required quota)
        """
        try:
            cls.quota_lock.acquire()

            cls.update_quota()

            q1 = cls.quota_stat_q1
            q2 = cls.quota_stat_q2
            if quota_type == 1:  # 非交易时段任务
                if q1 > count:
                    cls.quota_stat_q1 = q1 - count
                    return True, q1 - count, count
                else:
                    return False, q1, count
            else:
                if q2 > count:
                    cls.quota_stat_q2 = q2 - count
                    return True, q2 - count, count
                else:
                    return False, q2, count
        finally:
            cls.quota_lock.release()
