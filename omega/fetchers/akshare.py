import datetime

import akshare as ak
import arrow
import pandas as pd
from coretypes import SecurityInfoSchema
from pandera.typing import DataFrame
from pypinyin import Style, lazy_pinyin


def _get_pinyin_initials(hanz: str)->str:
    """将汉字转换为拼音首字母
    
    本辅助函数用以获取证券名的拼音简写。
    """
    return "".join(lazy_pinyin(hanz, style = Style.FIRST_LETTER)).upper()

def fetch_stock_list()->DataFrame[SecurityInfoSchema]:
    """获取证券列表
    
    此接口用时以秒计，因此一般需要在工作者进程中调用。
    """
    # 获取沪市股票列表
    sh = ak.stock_info_sh_name_code()
    sh = sh.rename({
        "证券简称": "alias", 
        "上市日期": "ipo", 
        "证券代码": "code"
    }, axis=1)

    sh["code"] = sh["code"].apply(lambda x: x+".SH")
    sh["initials"] = sh["alias"].apply(_get_pinyin_initials)
    sh["exit"] = None
    sh["type"] = "stock"
    
    sh = sh[["code", "alias", "initials", "ipo", "exit", "type"]]

    # 补齐退市证券
    sh_delisted = ak.stock_info_sh_delist()
    sh_delisted = sh_delisted.rename({
            "公司代码": "code",
            "公司简称": "alias",
            "上市日期": "ipo",
            "暂停上市日期": "exit"
        }, axis=1)

    sh_delisted["type"] = "stock"
    sh_delisted["code"] = sh_delisted["code"].apply(lambda x: x + ".SH")
    sh_delisted["initials"] = sh_delisted["alias"].apply(_get_pinyin_initials)
    sh_delisted = sh_delisted[["code", "alias", "initials", "ipo", "exit", "type"]]

    # 获取深市股票列表
    sz = ak.stock_info_sz_name_code()
    sz = sz.rename({
        "A股代码": "code",
        "A股简称": "alias",
        "A股上市日期": "ipo"
    }, axis=1)

    sz["code"] = sz["code"].apply(lambda x: x+".SZ")
    sz["initials"] = sz["alias"].apply(_get_pinyin_initials)
    sz["type"] = "stock"

    # akshare返回的深市股票，日期列为str
    sz["ipo"] = sz["ipo"].apply(lambda x: arrow.get(x).date())

    sz["exit"] = None
    sz = sz[["code", "alias", "initials", "ipo", "exit", "type"]]

    # 补齐深市退市证券
    sz_delisted = ak.stock_info_sz_delist("终止上市公司")
    sz_delisted = sz_delisted.rename({
        "证券代码": "code",
        "证券简称": "alias",
        "上市日期": "ipo",
        "终止上市日期": "exit"
    }, axis=1)

    sz_delisted["type"] = "stock"
    sz_delisted["code"] = sz_delisted["code"].apply(lambda x: x + ".SZ")
    sz_delisted["initials"] = sz_delisted["alias"].apply(_get_pinyin_initials)
    sz_delisted = sz_delisted[["code", "alias", "initials", "ipo", "exit", "type"]]

    # 北交所
    bj = ak.stock_info_bj_name_code()
    bj = bj.rename({
        "证券代码": "code",
        "证券简称": "alias",
        "上市日期": "ipo"
    }, axis=1)

    bj["code"] = bj["code"].apply(lambda x: x+".BJ")
    bj["initials"] = bj["alias"].apply(_get_pinyin_initials)
    bj["type"] = "stock"
    bj["exit"] = None

    bj = bj[["code", "alias", "initials", "ipo", "exit", "type"]]

    # 指数列表
    index = ak.index_stock_info()
    index = index.rename({
        "index_code": "code",
        "display_name": "alias",
        "publish_date": "ipo"
    }, axis=1)

    index["code"] = index["code"].apply(lambda x: x+".SH" if x.startswith("000") else x+".SZ")
    index["initials"] = index["alias"].apply(_get_pinyin_initials)
    index["exit"] = None
    index["type"] = "index"
    index["ipo"] = index["ipo"].apply(lambda x: arrow.get(x).date())
    
    return pd.concat([sh, sz, sh_delisted, sz_delisted, index], axis=0) #type: ignore
