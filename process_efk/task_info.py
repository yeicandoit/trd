import requests
import json
import logging.config
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_TASK_INFO = "http://localhost:9200/task_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/task_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "task_name":{"type":"keyword"}, "task_id":{"type":"long"}, "num":{"type":"long"}, "num_for_no_child":{"type":"long"}, "num_for_parent":{"type":"long"}}}'


def get_task_info(nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)

    sql_task_name = "select id, name from user_tasks"
    rt = mysql_tool.querydb(sql_task_name, logger, sql_task_name)
    for v in rt:
        data[v[0]] = {}
        data[v[0]]['task_id'] = int(v[0])
        data[v[0]]['task_name'] = v[1]

    sql_task_num = "select task_id, sum(today_count) from user_task_day_records where day = \"%s\" group by task_id" % day1
    rt = mysql_tool.querydb(sql_task_num, logger, sql_task_num)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['num'] = int(v[1])

    sql_task_parent_num = "select task_id, sum(today_count) from user_task_day_records where user_id in (select distinct parent_id from users) and day = \"%s\" group by task_id" % day1
    rt = mysql_tool.querydb(sql_task_parent_num, logger, sql_task_parent_num)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['num_for_parent'] = int(v[1])

    return data


def process(nday=1):
    mysql_tool.connectdb()
    mysql_tool.querydb("SET NAMES utf8mb4")
    data = get_task_info(nday)
    mysql_tool.closedb()

    for k, v in data.items():
        v['@timestamp'] = time_tool.get_someday_es_format(-nday)
        if 'num' in v.keys() and 'num_for_parent' in v.keys():
            v['num_for_no_child'] = v['num'] - v['num_for_parent']
        _id = time_tool.get_someday_str(-nday)
        url = URL_ELASTICSEARCH_TASK_INFO + "/" + _id + "_" + str(k)
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(v), timeout=(10, 20))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request task_info index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)


if __name__ == '__main__':
    process()
