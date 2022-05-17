from threading import Lock

work_state = (
    {}
)  # work的状态,{'account': {'account': xxxx, 'quota': xxx, 'impl': 'jq', 'time': '2022-05-11 15:20:29'}}
today_quota_stat = {"quota1": 0, "quota2": 0, "date": None}  # 当日剩余配额
quota_lock = Lock()  # 读写上面的quota_stat时，需要加锁
