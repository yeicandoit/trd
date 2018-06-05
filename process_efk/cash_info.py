import requests
import json
import logging.config
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_CASH_INFO = "http://localhost:9200/cash_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/cash_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "num_user":{"type":"long"},"num_user_new":{"type":"long"}, "average_time_first":{"type":"float"}, "count":{"type":"long"}, "count_5yuan":{"type":"long"}, "average_time_first_5yuan":{"type":"float"}, "count_failure":{"type":"long"}, "num_norm":{"type":"long"}, "cash_norm":{"type":"long"}, "num_blacklist":{"type":"long"}, "cash_blacklist":{"type":"long"}, "num_cheating":{"type":"long"}, "cash_cheating":{"type":"long"}, "cash_total":{"type":"long"}, "cash_out":{"type":"long"}, "point":{"type":"long"}}}'


def get_cash_user(nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)

    sql_all = "select count(*), count(distinct user_id), sum(price) from user_withdraw_orders where created_at >= \"%s\" and created_at < \"%s\"" % (day1, day2)
    rt = mysql_tool.querydb(sql_all, logger, sql_all)
    if len(rt) > 0:
        data['count'] = int(rt[0][0])
        data['num_user'] = int(rt[0][1])
        data['cash_total'] = int(rt[0][2]) / 100

    sql_failure_count = "select count(*) from user_withdraw_orders where status = 1 and withdraw_status = 1 and updated_at >= \"%s\" and updated_at < \"%s\"" % (day1, day2)
    rt = mysql_tool.querydb(sql_failure_count, logger, sql_failure_count)
    if len(rt) > 0:
        data['count_failure'] = rt[0][0]

    sql_norm = "select count(distinct uwo.user_id), sum(uwo.price) from user_withdraw_orders as uwo left join users as u on (uwo.user_id = u.id) where uwo.created_at >= \"%s\" and uwo.created_at < \"%s\" and u.user_type = 0" % (day1, day2)
    rt = mysql_tool.querydb(sql_norm, logger, sql_norm)
    if len(rt) > 0:
        data['num_norm'] = int(rt[0][0])
        data['cash_norm'] = int(rt[0][1]) / 100

    sql_black = "select count(distinct uwo.user_id), sum(uwo.price) from user_withdraw_orders as uwo left join users as u on (uwo.user_id = u.id) where uwo.created_at >= \"%s\" and uwo.created_at < \"%s\" and u.user_type = 1" % (day1, day2)
    rt = mysql_tool.querydb(sql_black, logger, sql_black)
    if len(rt) > 0:
        data['num_blacklist'] = int(rt[0][0])
        data['cash_blacklist'] = int(rt[0][1]) / 100

    sql_cheating = "select count(distinct uwo.user_id), sum(uwo.price) from user_withdraw_orders as uwo left join users as u on (uwo.user_id = u.id) where uwo.created_at >= \"%s\" and uwo.created_at < \"%s\" and u.user_type = 4" % (day1, day2)
    rt = mysql_tool.querydb(sql_cheating, logger, sql_cheating)
    if len(rt) > 0:
        data['num_cheating'] = int(rt[0][0])
        data['cash_cheating'] = int(rt[0][1]) / 100

    sql_cash = "select sum(price) from user_withdraw_orders where withdraw_status = 3 and updated_at >= \"%s\" and updated_at < \"%s\"" % (
        day1, day2)
    rt = mysql_tool.querydb(sql_cash, logger, sql_cash)
    if len(rt) > 0:
        data['cash_out'] = int(rt[0][0]) / 100

    sql_point = "select sum(point) from user_point_bills where created_at >= \"%s\" and created_at < \"%s\"" % (
        day1, day2)
    rt = mysql_tool.querydb(sql_point, logger, sql_point)
    if len(rt) > 0:
        data['point'] = int(rt[0][0])

    sql_new_cash_user = "select count(distinct uwo.user_id), sum(datediff(uwo.created_at, u.registered_at)) from user_withdraw_orders as uwo join users as u on (uwo.user_id = u.id) where uwo.user_id not in (select distinct user_id from user_withdraw_orders where created_at < \"%s\") and  uwo.created_at >=\"%s\" and uwo.created_at < \"%s\"" % (day1, day1, day2)
    rt = mysql_tool.querydb(sql_new_cash_user, logger, sql_new_cash_user)
    if len(rt) > 0:
        data['num_user_new'] = int(rt[0][0])
        if rt[0][0] > 0:
            data['average_time_first'] = float(rt[0][1] / rt[0][0])

    sql_new_5_yuan_user = "select count(ufy.user_id), sum(datediff(ufy.updated_at, u.registered_at)) from user_five_yuan_withdraw_cash_task_records as ufy join users as u on (ufy.user_id = u.id) where ufy.updated_at >= \"%s\" and ufy.updated_at < \"%s\" and ufy.is_withdraw = 1" % (day1, day2)
    rt = mysql_tool.querydb(sql_new_5_yuan_user, logger, sql_new_5_yuan_user)
    if len(rt) > 0:
        data['count_5yuan'] = rt[0][0]
        if rt[0][0] > 0:
            data['average_time_first_5yuan'] = float(rt[0][1] / rt[0][0])

    return data


def process(nday=1):
    mysql_tool.connectdb()
    data = get_cash_user(nday)
    mysql_tool.closedb()
    data['@timestamp'] = time_tool.get_someday_es_format(-nday)
    _id = time_tool.get_someday_str(-nday)
    url = URL_ELASTICSEARCH_CASH_INFO + "/" + _id
    r = requests.post(url, headers=JSON_HEADER,
                      data=json.dumps(data), timeout=(10, 20))
    if 200 != r.status_code and 201 != r.status_code:
        logger.error("request active_user_info index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)


if __name__ == '__main__':
    process()
