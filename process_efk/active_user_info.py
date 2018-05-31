import requests
import json
import time
import logging.config
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_ACTIVE_USER_INFO = "http://localhost:9200/active_user_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/active_user_info/doc/_mapping -d '{"properties": {"channel":{"type":"keyword"}, "@timestamp":{"type":"date"}, "num_device":{"type":"long"},"num_user":{"type":"long"}, "num_task_average":{"type":"float"}, "num_read":{"type":"long"}, "num_video":{"type":"long"}, "num_read_average":{"type":"float"}, "app_stay_first":{"type":"long"}, "app_stay":{"type":"long"}, "num_child":{"type":"long"}}}'


def get_query_user_num():
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [],
                "must_not": [
                    {
                        "match_phrase": {
                            "user_id.keyword": {
                                "query": "-1"
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "per_channel": {
                "terms": {
                    "field": "channel.keyword",
                    "size": 100000
                },
                "aggs": {
                    "user_num": {
                        "cardinality": {
                            "field": "user_id.keyword",
                        }
                    }
                }
            }
        }
    }
    return query


def get_query_device_num():
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [],
                "must_not": [
                    {
                        "match_phrase": {
                            "device_id.keyword": {
                                "query": ""
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "per_channel": {
                "terms": {
                    "field": "channel.keyword",
                    "size": 100000
                },
                "aggs": {
                    "device_num": {
                        "cardinality": {
                            "field": "device_id.keyword",
                        }
                    }
                }
            }
        }
    }
    return query


def get_query_app_stay():
    query = {
        "size": 0,
        "aggs": {
            "per_channel": {
                "terms": {
                    "field": "channel.keyword",
                    "size": 10000000
                },
                "aggs": {
                    "sum_time": {
                        "sum": {
                            "field": "use_time",
                        }
                    },
                    "user_num": {
                        "cardinality": {
                            "field": "user_id.keyword",
                        }
                    }
                }
            }
        },
        "query": {
            "bool": {
                "must": [
                    {
                        "match_phrase": {
                            "key.keyword": {
                                "query": "app_common_use_time"
                            }
                        }
                    }
                ],
                "must_not": [
                    {
                        "match_phrase": {
                            "use_time": {
                                "query": -1
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            "user_id.keyword": {
                                "query": "-1"
                            }
                        }
                    }
                ]
            }
        }
    }

    return query


def get_user(query={}, nday=1):
    data = {}
    start = time_tool.get_weehours_of_someday(-nday)
    query["query"]["bool"]["must"] = [
        {
            "range": {
                "@timestamp": {
                    "gte": start * 1000,
                    "lte": (start + 86400) * 1000 - 1,
                    "format": "epoch_millis"
                }

            }
        }
    ]
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        for v in r_json['aggregations']['per_channel']['buckets']:
            data[v['key']] = v['user_num']['value']
    else:
        logger.error("request user index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return data


def get_device(query={}, nday=1):
    data = {}
    start = time_tool.get_weehours_of_someday(-nday)
    query["query"]["bool"]["must"] = [
        {
            "range": {
                "@timestamp": {
                    "gte": start * 1000,
                    "lte": (start + 86400) * 1000 - 1,
                    "format": "epoch_millis"
                }

            }
        }
    ]

    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(10, 20))
    if 200 == r.status_code:
        r_json = r.json()
        for v in r_json['aggregations']['per_channel']['buckets']:
            data[v['key']] = v['device_num']['value']
    else:
        logger.error("request device index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return data


def get_app_stay(query={}, nday=1):
    app_stay = {}
    app_stay_first = {}
    start = time_tool.get_weehours_of_someday(-nday)
    time_range = {
        "range": {
            "@timestamp": {
                "gte": start * 1000,
                "lte": (start + 86400) * 1000 - 1,
                "format": "epoch_millis"
            }
        }
    }

    query["query"]["bool"]["must"].append(time_range)
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        for v in r_json['aggregations']['per_channel']['buckets']:
            app_stay[v['key']] = v['sum_time']['value'] / \
                float(v['user_num']['value'])
    else:
        logger.error("request user index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    query_is_first = {
        "match_phrase": {
            "is_first.keyword": {
                "query": "true"
            }
        }
    }
    query["query"]["bool"]["must"].append(query_is_first)
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        for v in r_json['aggregations']['per_channel']['buckets']:
            app_stay_first[v['key']] = v['sum_time']['value'] / \
                float(v['user_num']['value'])
    else:
        logger.error("request user index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return app_stay, app_stay_first


def get_user_arr(nday=1):
    start = time_tool.get_weehours_of_someday(-nday)
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start * 1000,
                                "lte": (start + 86400) * 1000 - 1,
                                "format": "epoch_millis"
                            }

                        }
                    }
                ],
                "must_not": [
                    {
                        "match_phrase": {
                            "user_id.keyword": {
                                "query": "-1"
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "user_arr": {
                "terms": {
                    "field": "user_id.keyword",
                    "size": 10000000
                }
            }
        }
    }

    data = []
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        data = [user['key']
                for user in r_json['aggregations']['user_arr']['buckets']]
    else:
        logger.error("request user index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return data


def get_task(user_arr=[], nday=1):
    data = {}
    day_str = time_tool.get_someday_str(-nday)
    sql = "select us.channel as channel, sum(ut.today_count) as count from user_statistics as us right join user_task_day_records as ut on (us.user_id = ut.user_id) where us.user_id in (%s) and ut.day = \"%s\" and ut.today_count > 0 group by us.channel"

    mysql_tool.connectdb()
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql), day_str)
        timestamp = int(time.time())
        rt = mysql_tool.querydb(sql_use)
        logger.info("get_task for 1000 users took %d second",
                    int(time.time()) - timestamp)
        for v in rt:
            if v[0] in data.keys():
                data[v[0]] += int(v[1])
            else:
                data[v[0]] = int(v[1])
    mysql_tool.closedb()

    return data


def get_reading(user_arr=[], nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    sql = "select us.channel as channel, count(nr.user_id) as count from user_statistics as us right join news_effective_readings as nr on (us.user_id = nr.user_id) where us.user_id in (%s) and nr.created_at >= \"%s\" and nr.created_at < \"%s\" and nr.effective = 1 group by us.channel"

    mysql_tool.connectdb()
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql), day1, day2)
        rt = mysql_tool.querydb(sql_use)
        for v in rt:
            if v[0] in data.keys():
                data[v[0]] += int(v[1])
            else:
                data[v[0]] = int(v[1])
    mysql_tool.closedb()

    return data


def get_video(user_arr=[], nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    sql = "select us.channel as channel, count(vr.user_id) as count from user_statistics as us right join video_effective_readings as vr on (us.user_id = vr.user_id) where us.user_id in (%s) and vr.created_at >= \"%s\" and vr.created_at < \"%s\" and vr.effective = 1 group by us.channel"

    mysql_tool.connectdb()
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql), day1, day2)
        rt = mysql_tool.querydb(sql_use)
        for v in rt:
            if v[0] in data.keys():
                data[v[0]] += int(v[1])
            else:
                data[v[0]] = int(v[1])
        print data
    mysql_tool.closedb()

    return data


def get_child(user_arr=[], nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    sql = "select us.channel, count(us.channel) from users as u left join user_statistics as us on (u.id = us.user_id) where u.parent_id in (%s) and u.bind_parent_at >= \"%s\" and u.bind_parent_at < \"%s\" group by us.channel"

    mysql_tool.connectdb()
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql), day1, day2)
        rt = mysql_tool.querydb(sql_use)
        for v in rt:
            if v[0] in data.keys():
                data[v[0]] += int(v[1])
            else:
                data[v[0]] = int(v[1])
    mysql_tool.closedb()

    return data


def process(nday=1):
    user_per_channel = get_user(query=get_query_user_num(), nday=nday)
    device_per_channel = get_device(
        query=get_query_device_num(), nday=nday)
    app_stay, app_stay_first = get_app_stay(get_query_app_stay(), nday)
    user_arr = get_user_arr()
    task_per_channel = get_task(user_arr, nday=nday)
    reading_per_channel = get_reading(user_arr, nday=nday)
    video_per_channel = get_video(user_arr, nday=nday)
    child_per_channel = get_child(user_arr, nday=nday)

    for k, v in user_per_channel.items():
        data = {}
        data["channel"] = k
        data["@timestamp"] = time_tool.get_someday_es_format(-nday)
        data["num_user"] = v
        if k in device_per_channel.keys():
            data["num_device"] = device_per_channel[k]
        if k in task_per_channel.keys() and v > 0:
            data["num_task_average"] = task_per_channel[k] / float(v)
        if k in reading_per_channel.keys():
            data["num_read"] = reading_per_channel[k]
            if v > 0:
                data["num_read_average"] = reading_per_channel[k] / float(v)
        if k in video_per_channel.keys():
            data["num_video"] = video_per_channel[k]
        if k in child_per_channel.keys():
            data["num_child"] = child_per_channel[k]
        if k in app_stay.keys():
            data["app_stay"] = int(app_stay[k])
        if k in app_stay_first.keys():
            data["app_stay_first"] = int(app_stay_first[k])
        url = URL_ELASTICSEARCH_ACTIVE_USER_INFO + "/" + \
            time_tool.get_someday_str(-nday) + "_" + k
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(data), timeout=(10, 20))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request active_user_info index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)


if __name__ == '__main__':
    process()
