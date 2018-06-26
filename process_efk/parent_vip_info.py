# coding=utf-8
import requests
import json
import logging.config
import new_user_info as nui
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_PARENT_VIP_INFO = "http://localhost:9200/parent_vip_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/parent_vip_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "vip":{"type":"keyword"}, "num":{"type":"long"}, "num_new":{"type":"long"}, "num_user_new":{"type":"long"}}}'


def get_parent_info(nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)

    # 各vip等级师傅数量
    sql_vip_parent_count = "select level, count(id) from users where id in (select distinct parent_id from users where bind_parent_at < \"%s\") group by level" % day2
    rt = mysql_tool.querydb(sql_vip_parent_count,
                            logger, sql_vip_parent_count)
    for v in rt:
        k = str(v[0])
        if k in data.keys():
            data[k]['num'] = int(v[1])
        else:
            data[k] = {}
            data[k]['num'] = int(v[1])

    return data


def process(nday=1):
    mysql_tool.connectdb()
    data = get_parent_info(nday)
    mysql_tool.closedb()

    for k, v in data.items():
        # 获取前一天各vip等级师傅数量, 各vip新增师傅数量 = 今日各vip师傅数量 - 昨天各vip师傅数量
        yid = time_tool.get_someday_str(-nday-1)
        url = URL_ELASTICSEARCH_PARENT_VIP_INFO + "/" + yid + "_" + k
        r = requests.get(url, headers=JSON_HEADER, timeout=(30, 120))
        if 200 != r.status_code:
            logger.error("request parent_info index failed, status_code:%d, reason:%s, k:%s",
                         r.status_code, r.reason, k)
        else:
            r_json = r.json()
            if "_source" in r_json.keys():
                yd = r_json["_source"]
                if "num" in yd.keys():
                    v["num_new"] = v["num"] - yd["num"]
        v['@timestamp'] = time_tool.get_someday_es_format(-nday)
        v["vip"] = k
        _id = time_tool.get_someday_str(-nday)
        url = URL_ELASTICSEARCH_PARENT_VIP_INFO + "/" + _id + "_" + k
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(v), timeout=(30, 120))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request parent_info index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)
    data_num_user = {}
    data_num_user['@timestamp'] = time_tool.get_someday_es_format(-nday)
    user_per_channel = nui.get_new_user(nday=nday)
    data_num_user["num_user_new"] = user_per_channel["all_channel"]
    data_num_user["vip"] = ""
    url = URL_ELASTICSEARCH_PARENT_VIP_INFO + "/" + time_tool.get_someday_str(-nday)
    r = requests.post(url, headers=JSON_HEADER,
                      data=json.dumps(data_num_user), timeout=(30, 120))
    if 200 != r.status_code and 201 != r.status_code:
        logger.error("request parent_info index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)


if __name__ == '__main__':
    process()
