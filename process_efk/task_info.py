# coding=utf-8
import requests
import json
import logging.config
import active
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_TASK_INFO = "http://localhost:9200/task_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/task_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "task_name":{"type":"keyword"}, "task_id":{"type":"long"}, "num":{"type":"float"}, "num_for_no_child":{"type":"float"}, "num_for_parent":{"type":"float"}, "n":{"type":"float"}, "n_for_no_child":{"type":"float"}, "n_for_parent":{"type":"float"}, "num_dau":{"type":"float"}}}'


def get_task_info(nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)

    sql_task_name = "select id, name from user_tasks"
    rt = mysql_tool.querydb(sql_task_name, logger, sql_task_name)
    for v in rt:
        data[v[0]] = {}
        data[v[0]]['task_id'] = int(v[0])
        data[v[0]]['task_name'] = v[1]

    sql_task_num = "select task_id, sum(today_count), count(distinct user_id) from user_task_day_records where day = \"%s\" and today_count !=0 group by task_id" % day1
    rt = mysql_tool.querydb(sql_task_num, logger, sql_task_num)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['num'] = int(v[1])
            data[v[0]]['n'] = int(v[2])

    sql_task_parent_num = "select task_id, sum(today_count), count(distinct user_id) from user_task_day_records where user_id in (select distinct parent_id from users where bind_parent_at < \"%s\") and day = \"%s\" and today_count !=0 group by task_id" % (day2, day1)
    rt = mysql_tool.querydb(sql_task_parent_num, logger, sql_task_parent_num)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['num_for_parent'] = int(v[1])
            data[v[0]]['n_for_parent'] = int(v[2])

    sql = "select sum(today_count), count(distinct user_id) from user_task_day_records where day = \"%s\" and user_id in (select id from users where registered_at >= \"%s\" and registered_at < \"%s\") and task_id in(1,2,3,22,4,5,29,30,31,33)" % (day1, day1, day2)
    rta = mysql_tool.querydb(sql, logger, sql)
    data[0] = {}
    data[0]['task_id'] = 0
    data[0]['task_name'] = u"今日新增注册用户完成新手任务平均个数"
    if rta[0][1] > 0:
        data[0]['num'] = round(float(rta[0][0]) / float(rta[0][1]), 2)
    sql = "select sum(today_count), count(distinct user_id) from user_task_day_records where day = \"%s\" and user_id in (select id from users where id in (select distinct parent_id from users where bind_parent_at >= \"%s\" and bind_parent_at < \"%s\") and registered_at >= \"%s\" and registered_at < \"%s\") and task_id in(1,2,3,22,4,5,29,30,31,33)" % (day1, day1, day2, day1, day2)
    rtp = mysql_tool.querydb(sql, logger, sql)
    if rtp[0][1] > 0:
        data[0]['num_for_parent'] = round(float(rtp[0][0]) / float(rtp[0][1]), 2)
    if rta[0][1] - rtp[0][1] > 0:
        data[0]['num_for_no_child'] = round(float(
            rta[0][0] - rtp[0][0]) / float(rta[0][1] - rtp[0][1]), 2)

    return data


def process(nday=1):
    mysql_tool.connectdb()
    mysql_tool.querydb("SET NAMES utf8mb4")
    data = get_task_info(nday)
    mysql_tool.closedb()

    dau, _ = active.get_user_device_count(active.get_query(-nday)) 

    for k, v in data.items():
        v['@timestamp'] = time_tool.get_someday_es_format(-nday)
        if k != 0 and 'num' in v.keys() and 'num_for_parent' in v.keys():
            v['num_for_no_child'] = v['num'] - v['num_for_parent']
            v['n_for_no_child'] = v['n'] - v['n_for_parent']
            if dau > 0:
                v['num_dau'] = float(v['num']) / dau
        _id = time_tool.get_someday_str(-nday)
        url = URL_ELASTICSEARCH_TASK_INFO + "/" + _id + "_" + str(k)
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(v), timeout=(10, 120))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request task_info index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)


if __name__ == '__main__':
    process()
