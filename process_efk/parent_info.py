# coding=utf-8
import requests
import json
import logging.config
import active_user_info
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_PARENT_INFO = "http://localhost:9200/parent_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/parent_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "channel":{"type":"keyword"}, "num_user":{"type":"long"}, "num_parent":{"type":"long"}, "percent_parent":{"type":"float"}, "num_active_parent":{"type":"long"}, "percent_active_parent":{"type":"float"}, "total_income":{"type":"long"}, "total_rebate":{"type":"long"}, "num_child":{"type":"long"}, "num_new_child":{"type":"long"}, "average_child_per_parent":{"type":"float"}, "average_rebate_per_child":{"type":"float"}, "num_new_parent":{"type":"long"},"num_child_increase_parent":{"type":"long"},"num_active_child":{"type":"long"},"today_income":{"type":"long"},"today_rebate":{"type":"long"}}}'


def get_active_child(nday=1):
    data = {}
    all_child = []
    user_arr = active_user_info.get_user_arr(nday)
    sql = "select channel, user_id from user_statistics where user_id in (select id from users where id in (%s) and parent_id != 0)"
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql))
        rt = mysql_tool.querydb(sql_use)
        for v in rt:
            all_child.append(v[1])
            if v[0] in data.keys():
                data[v[0]].append(v[1])
            else:
                data[v[0]] = []
                data[v[0]].append(v[1])
    active_child = {}
    for k, v in data.items():
        active_child[k] = len(list(set(data[k])))
    active_child["all_channel"] = len(list(set(all_child)))

    return active_child


def get_parent_info(nday=1):
    data = {}
    data["all_channel"] = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)

    # 当日新增的师傅
    sql_channel_new_parent_count = "select us.channel, count(distinct u.parent_id) from users as u join user_statistics as us on (u.id = us.user_id) where u.bind_parent_at >= \"%s\" and u.bind_parent_at < \"%s\" and u.parent_id not in (select distinct parent_id from users where bind_parent_at < \"%s\") group by us.channel" % (
        day1, day2, day1)
    rt = mysql_tool.querydb(sql_channel_new_parent_count,
                            logger, sql_channel_new_parent_count)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['num_new_parent'] = int(v[1])
        else:
            data[v[0]] = {}
            data[v[0]]['num_new_parent'] = int(v[1])
    sql_new_parent_count = "select count(distinct parent_id) from users where bind_parent_at >= \"%s\" and bind_parent_at < \"%s\" and parent_id not in (select distinct parent_id from users where bind_parent_at < \"%s\")" % (
        day1, day2, day1)
    rt = mysql_tool.querydb(sql_new_parent_count, logger, sql_new_parent_count)
    data["all_channel"]["num_new_parent"] = int(rt[0][0])

    # 当日徒弟有增加师傅
    sql_channel_child_increase_parent = "select us.channel, count(distinct u.parent_id) from users as u join user_statistics as us on (u.id = us.user_id) where u.bind_parent_at >= \"%s\" and u.bind_parent_at < \"%s\" group by us.channel" % (
        day1, day2)
    rt = mysql_tool.querydb(sql_channel_child_increase_parent,
                            logger, sql_channel_child_increase_parent)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]["num_child_increase_parent"] = int(v[1])
        else:
            data[v[0]] = {}
            data[v[0]]["num_child_increase_parent"] = int(v[1])
    sql_child_increase_parent = "select count(distinct parent_id) from users where  bind_parent_at >= \"%s\" and bind_parent_at < \"%s\"" % (
        day1, day2)
    rt = mysql_tool.querydb(sql_child_increase_parent,
                            logger, sql_child_increase_parent)
    data["all_channel"]["num_child_increase_parent"] = int(rt[0][0])

    # 当日新增徒弟
    sql_channel_new_child_count = "select us.channel, count(u.id) from users as u join user_statistics as us on (u.id = us.user_id) where u.bind_parent_at >= \"%s\" and u.bind_parent_at< \"%s\" group by us.channel;" % (
        day1, day2)
    rt = mysql_tool.querydb(sql_channel_new_child_count,
                            logger, sql_channel_new_child_count)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['num_new_child'] = int(v[1])
        else:
            data[v[0]] = {}
            data[v[0]]['num_new_child'] = int(v[1])
    sql_new_child_count = "select count(*) from users where bind_parent_at >= \"%s\" and bind_parent_at < \"%s\"" % (
        day1, day2)
    rt = mysql_tool.querydb(sql_new_child_count, logger, sql_new_child_count)
    data["all_channel"]['num_new_child'] = int(rt[0][0])

    # 师傅的总收入
    sql_channel_income = "select channel, sum(total_point_income) from user_statistics where user_id in (select distinct parent_id from users) group by channel"
    rt = mysql_tool.querydb(sql_channel_income, logger, sql_channel_income)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['total_income'] = int(v[1])
        else:
            data[v[0]] = {}
            data[v[0]]['total_income'] = int(v[1])
    sql_income = "select sum(total_point_income) from user_statistics where user_id in (select distinct parent_id from users)"
    rt = mysql_tool.querydb(sql_income, logger, sql_income)
    data["all_channel"]['total_income'] = int(rt[0][0])

    # 师傅的进贡收入
    sql_channel_rebate = "select channel, sum(child_point_rebate) from user_statistics where user_id in (select distinct parent_id from users) group by channel"
    rt = mysql_tool.querydb(sql_channel_rebate, logger, sql_channel_rebate)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['total_rebate'] = int(v[1])
        else:
            data[v[0]] = {}
            data[v[0]]['total_rebate'] = int(v[1])
    sql_rebate = "select sum(child_point_rebate) from user_statistics where user_id in (select distinct parent_id from users)"
    rt = mysql_tool.querydb(sql_rebate, logger, sql_rebate)
    data["all_channel"]['total_rebate'] = int(rt[0][0])

    # 当日登陆徒弟
    active_child = get_active_child(nday)
    for k, v in data.items():
        if "num_child_increase_parent" in v.keys() and "num_new_child" in v.keys() and v["num_child_increase_parent"] > 0:
            v["average_child_per_parent"] = v["num_new_child"] / \
                float(v["num_child_increase_parent"])
        if k in active_child.keys():
            v["num_active_child"] = active_child[k]

    return data


def process(nday=1):
    mysql_tool.connectdb()
    data = get_parent_info(nday)
    mysql_tool.closedb()

    for k, v in data.items():
        # 获取前一天师傅的总收入和师傅的进贡收入来计算当日的收入
        yid = time_tool.get_someday_str(-nday-1)
        url = URL_ELASTICSEARCH_PARENT_INFO + "/" + yid + "_" + k
        r = requests.get(url, headers=JSON_HEADER, timeout=(10, 20))
        if 200 != r.status_code:
            logger.error("request parent_info index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)
        else:
            r_json = r.json()
            if "_source" in r_json.keys():
                yd = r_json["_source"]
                if "total_income" in v.keys():
                    ydti = yd["total_income"] if "total_income" in yd.keys() else 0
                    v["today_income"] = v["total_income"] - ydti
                if "total_rebate" in v.keys():
                    ydtr = yd["total_rebate"] if "total_rebate" in yd.keys() else 0
                    v["today_rebate"] = v["total_rebate"] - ydtr
                    if "num_active_child" in v.keys() and v["num_active_child"] > 0:
                          v["average_rebate_per_child"] = v["today_rebate"] / \
                              float(v["num_active_child"])
        v['@timestamp'] = time_tool.get_someday_es_format(-nday)
        v["channel"] = k
        _id = time_tool.get_someday_str(-nday)
        url = URL_ELASTICSEARCH_PARENT_INFO + "/" + _id + "_" + k
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(v), timeout=(10, 20))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request parent_info index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)


if __name__ == '__main__':
    process()
