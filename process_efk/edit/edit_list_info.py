# coding=utf-8
import requests
import json
import define
from conf import config
from util import mysql_tool, time_tool, log_tool


logger = log_tool.logger


# 获取榜单pv数据
def get_hot_list_pv_info(nday=1):
    data = {}

    for table, category_id in [("news_hot_seven_day_lists", define.NEWS_HOT_SEVEN),
                               ("news_hot_total_lists", define.NEWS_HOT_TOTAL)]:
        # 获取pv.
        sql = "select sum(real_pv) from news where id in (select news_id from %s)" % table
        rt = mysql_tool.querydb(sql, logger, sql)
        for v in rt:
            data[category_id] = int(v[0])

    return data


def process(nday=1):
    mysql_tool.connectdb(host="47.96.238.205", database="taozuiredian-news")
    list_pv_total = get_hot_list_pv_info(nday)
    mysql_tool.closedb()

    for k, v in list_pv_total.items():
        # 总榜和蹿红榜的所有资讯会在凌晨一点更新，这样在凌晨计算的这两个榜的pv_total就
        # 失效了, 需要重新计算一次
        _id = time_tool.get_someday_str(-nday)
        url = config.URL_EDIT_INFO + "/" + _id + "_news_" + str(k)
        r = requests.get(url, headers=config.JSON_HEADER, timeout=(30, 120))
        if 200 != r.status_code:
            logger.error("request edit_info index failed, status_code:%d, reason:%s, k:%s",
                         r.status_code, r.reason, k)
        else:
            r_json = r.json()
            if "_source" in r_json.keys():
                yd = r_json["_source"]
                yd['pv_total'] = v
                yd['@timestamp'] = time_tool.get_someday_es_format(-nday)
                url = config.URL_EDIT_INFO + "/" + _id + "_news_" + str(k)
                r = requests.post(url, headers=config.JSON_HEADER,
                                  data=json.dumps(yd), timeout=(30, 120))
                if 200 != r.status_code and 201 != r.status_code:
                    logger.error("request edit index failed, status_code:%d, reason:%s, %s, %s",
                                 r.status_code, json.dumps(r.json()), url, json.dumps(v))


if __name__ == '__main__':
    process()
