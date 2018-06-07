import requests
import json
import logging.config
import active_user_info
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_PARENT_INFO = "http://localhost:9200/parent_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/parent_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "channel":{"type":"keyword"}, "num_user":{"type":"long"}, "num_parent":{"type":"long"}, "percent_parent":{"type":"float"}, "num_active_parent":{"type":"long"}, "percent_active_parent":{"type":"float"}, "total_income":{"type":"long"}, "total_rebate":{"type":"long"}, "num_child":{"type":"long"}, "num_new_child":{"type":"long"}, "average_child_per_parent":{"type":"float"}, "average_rebate_per_child":{"type":"float"}}}'


def get_active_parent(nday=1):
    data = {}
    all_parent = []
    user_arr = active_user_info.get_user_arr(nday)
    sql = "select channel, user_id from user_statistics where user_id in (select distinct parent_id from users where id in (%s))"
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql))
        rt = mysql_tool.querydb(sql_use)
        for v in rt:
            all_parent.append(v[1])
            if v[0] in data.keys():
                data[v[0]].append(v[1])
            else:
                data[v[0]] = []
                data[v[0]].append(v[1])
    active_parent = {}
    for k, v in data.items():
        active_parent[k] = len(list(set(data[k])))
    active_parent["all_channel"] = len(list(set(all_parent)))

    return active_parent


def get_parent_info(nday=1):
    data = {}
    data["all_channel"] = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)

    sql_channel_count = "select us.channel, count(u.id) from users as u left join user_statistics as us on (u.id = us.user_id) where u.registered_at < \"%s\" group by us.channel" % day2
    rt = mysql_tool.querydb(sql_channel_count, logger, sql_channel_count)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['num_user'] = int(v[1])
        else:
            data[v[0]] = {}
            data[v[0]]['num_user'] = int(v[1])
    sql_count = "select count(*) from users where registered_at < \"%s\"" % day2
    rt = mysql_tool.querydb(sql_count, logger, sql_count)
    data["all_channel"]['num_user'] = int(rt[0][0])

    sql_channel_parent_count = "select channel, count(distinct user_id) from user_statistics where user_id in (select distinct parent_id from users where registered_at < \"%s\") group by channel" % day2
    rt = mysql_tool.querydb(sql_channel_parent_count,
                            logger, sql_channel_parent_count)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['num_parent'] = int(v[1])
        else:
            data[v[0]] = {}
            data[v[0]]['num_parent'] = int(v[1])
    sql_parent_count = "select count(distinct parent_id) from users where registered_at < \"%s\"" % day2
    rt = mysql_tool.querydb(sql_parent_count, logger, sql_parent_count)
    data["all_channel"]['num_parent'] = int(rt[0][0])

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

    sql_channel_child_count = "select us.channel, count(u.id) from users as u join user_statistics as us on (u.id = us.user_id) where u.parent_id != 0 and u.registered_at < \"%s\" group by us.channel;" % day2
    rt = mysql_tool.querydb(sql_channel_child_count,
                            logger, sql_channel_child_count)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['num_child'] = int(v[1])
        else:
            data[v[0]] = {}
            data[v[0]]['num_child'] = int(v[1])
    sql_child_count = "select count(*) from users where parent_id != 0  and registered_at < \"%s\"" % day2
    rt = mysql_tool.querydb(sql_child_count,logger, sql_child_count)
    data["all_channel"]['num_child'] = int(rt[0][0])

    sql_channel_new_child_count = "select us.channel, count(u.id) from users as u join user_statistics as us on (u.id = us.user_id) where u.parent_id != 0 and u.registered_at >= \"%s\" and u.registered_at < \"%s\" group by us.channel;" % (
        day1, day2)
    rt = mysql_tool.querydb(sql_channel_new_child_count, logger, sql_channel_new_child_count)
    for v in rt:
        if v[0] in data.keys():
            data[v[0]]['num_new_child'] = int(v[1])
        else:
            data[v[0]] = {}
            data[v[0]]['num_new_child'] = int(v[1])
    sql_new_child_count = "select count(*) from users where parent_id != 0 and registered_at >= \"%s\" and registered_at < \"%s\"" % (day1, day2)
    rt = mysql_tool.querydb(sql_new_child_count, logger, sql_new_child_count)
    data["all_channel"]['num_new_child'] = int(rt[0][0])

    active_parent = get_active_parent(nday)
    for k, v in data.items():
        if "num_user" in v.keys() and "num_parent" in v.keys() and v["num_user"] > 0:
            v["percent_parent"] = v["num_parent"] / float(v["num_user"])
        if "num_parent" in v.keys() and k in active_parent.keys() and v["num_parent"] > 0:
            v["num_active_parent"] = active_parent[k]
            v["percent_active_parent"] = active_parent[k] / \
                float(v["num_parent"])
        if "num_parent" in v.keys() and "num_child" in v.keys() and v["num_parent"] > 0:
            v["average_child_per_parent"] = v["num_child"] / \
                float(v["num_parent"])
        if "num_child" in v.keys() and "total_rebate" in v.keys() and v["num_child"] > 0:
            v["average_rebate_per_child"] = v["total_rebate"] / \
                float(v["num_child"])

    return data


def process(nday=1):
    mysql_tool.connectdb()
    data = get_parent_info(nday)
    mysql_tool.closedb()

    for k, v in data.items():
        v['@timestamp'] = time_tool.get_someday_es_format(-nday)
        v["channel"] = k
        _id = time_tool.get_someday_str(-nday)
        url = URL_ELASTICSEARCH_PARENT_INFO + "/" + _id + "_" + k
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(v), timeout=(10, 20))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request active_user_info index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)


if __name__ == '__main__':
    process()
