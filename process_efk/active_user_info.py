# coding=utf-8
import requests
import json
import copy
import logging.config
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_USER = "http://localhost:9200/user/doc/_search"
URL_ELASTICSEARCH_DEVICE = "http://localhost:9200/device/doc/_search"
URL_ELASTICSEARCH_ACTIVE_USER_INFO = "http://localhost:9200/active_user_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/active_user_info/doc/_mapping -d '{"properties": {"channel":{"type":"keyword"}, "@timestamp":{"type":"date"}, "num_device":{"type":"long"},"num_user":{"type":"long"}, "num_task_average":{"type":"float"}, "num_read":{"type":"long"}, "num_read_user":{"type":"long"} "num_video":{"type":"long"}, "num_read_average":{"type":"float"}, "app_stay_first":{"type":"long"}, "app_stay":{"type":"long"}, "num_child":{"type":"long"}, "num_video_user":{"type":"long"}, "num_video_average":{"type":"long"}, "app_stay_first_show":{"type":"keyword"}, "app_stay_show":{"type":"keyword"}}}'


def get_query_app_stay():
    query = {
        "size": 0,
        "aggs": {
            "sum_time": {
                "sum": {
                    "field": "use_time",
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
                    }
                ],
                "filter": {
                    "terms": {
                        "user_id.keyword": []
                    }
                }
            }
        }
    }

    return query


def get_user_channel_(user_arr=[]):
    data = {}
    query = {
        "size": 0,
        "aggs": {
            "per_channel": {
                "terms": {
                    "field": "channel",
                    "size": 100000000
                },
                "aggs": {
                    "user_arr": {
                        "terms": {
                            "field": "user_id",
                            "size": 100000000
                        }
                    }
                }
            }
        },
        "query": {
            "bool": {
                "filter": {
                    "terms": {
                        "user_id": []
                    }
                }
            }
        }
    }
    for i in range(0, len(user_arr), 10000):
        user_to_query = user_arr[i:i+10000]
        query["query"]["bool"]["filter"]["terms"]["user_id"] = user_to_query
        r = requests.post(URL_ELASTICSEARCH_USER, headers=JSON_HEADER,
                          data=json.dumps(query), timeout=(30, 60))
        if 200 == r.status_code:
            r_json = r.json()
            for v in r_json['aggregations']['per_channel']['buckets']:
                if v['key'] not in data.keys():
                    data[v['key']] = [a['key']
                                      for a in v['user_arr']['buckets']]
                else:
                    arr = [a['key'] for a in v['user_arr']['buckets']]
                    data[v['key']] = list(set(data[v['key']]).union(set(arr)))
        else:
            logger.error("request user index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)

    return data


def get_device_channel(device_arr=[]):
    data = {}
    query = {
        "size": 0,
        "aggs": {
            "per_channel": {
                "terms": {
                    "field": "channel",
                    "size": 100000000
                },
                "aggs": {
                    "device_num": {
                        "cardinality": {
                            "field": "device_id.keyword",
                        }
                    }
                }
            }
        },
        "query": {
            "bool": {
                "filter": {
                    "terms": {
                        "device_id.keyword": []
                    }
                }
            }
        }
    }
    for i in range(0, len(device_arr), 10000):
        device_to_query = device_arr[i:i+10000]
        query["query"]["bool"]["filter"]["terms"]["device_id.keyword"] = device_to_query
        r = requests.post(URL_ELASTICSEARCH_DEVICE, headers=JSON_HEADER,
                          data=json.dumps(query), timeout=(30, 60))
        if 200 == r.status_code:
            r_json = r.json()
            for v in r_json['aggregations']['per_channel']['buckets']:
                if v['key'] not in data.keys():
                    data[v['key']] = v['device_num']['value']
                else:
                    data[v['key']] += v['device_num']['value']
        else:
            logger.error("request user index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)
    data["all_channel"] = len(device_arr)

    return data


def get_device_arr(nday=1):
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
                            "device_id.keyword": {
                                "query": ""
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "device_arr": {
                "terms": {
                    "field": "device_id.keyword",
                    "size": 100000000
                }
            }
        }
    }

    data = {}
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(10, 30))
    if 200 == r.status_code:
        r_json = r.json()
        data = [device['key']
                for device in r_json['aggregations']['device_arr']['buckets']]
    else:
        logger.error("request device index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return data


def get_app_stay(query={}, user_channel={}, nday=1):
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
    for k, user_arr in user_channel.items():
        app_stay[k] = 0
        for i in range(0, len(user_arr), 10000):
            user_to_query = user_arr[i:i+10000]
            query["query"]["bool"]["filter"]["terms"]["user_id.keyword"] = user_to_query
            r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                              data=json.dumps(query), timeout=(30, 60))
            if 200 == r.status_code:
                r_json = r.json()
                app_stay[k] += r_json['aggregations']['sum_time']['value']
            else:
                logger.error("request user index failed, status_code:%d, reason:%s",
                             r.status_code, r.reason)
        app_stay[k] = app_stay[k] / len(user_arr)

    query_is_first = {
        "match_phrase": {
            "is_first.keyword": {
                "query": "true"
            }
        }
    }
    query["query"]["bool"]["must"].append(query_is_first)
    for k, user_arr in user_channel.items():
        app_stay_first[k] = 0
        for i in range(0, len(user_arr), 10000):
            user_to_query = user_arr[i:i+10000]
            query["query"]["bool"]["filter"]["terms"]["user_id.keyword"] = user_to_query
            r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                              data=json.dumps(query), timeout=(30, 60))
            if 200 == r.status_code:
                r_json = r.json()
                app_stay_first[k] += r_json['aggregations']['sum_time']['value']
            else:
                logger.error("request user index failed, status_code:%d, reason:%s",
                             r.status_code, r.reason)
        app_stay_first[k] = app_stay_first[k] / len(user_arr)

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


def get_user_channel(user_arr=[]):
    data = {}
    sql = "select channel, user_id from user_statistics where user_id in (%s)"

    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql))
        rt = mysql_tool.querydb(
            sql_use, logger, "get user channel for %d users" % len(user_to_sql))
        for v in rt:
            if v[0] in data.keys():
                data[v[0]].append(str(v[1]))
            else:
                data[v[0]] = [str(v[1])]

    return data


def get_task(user_channel={}, nday=1):
    data = {}
    data["all_channel"] = {}
    data["all_channel"]["num_task"] = 0
    data["all_channel"]["num_user"] = 0
    day_str = time_tool.get_someday_str(-nday)
    sql = "select sum(today_count), count(distinct user_id) from user_task_day_records where user_id in (%s) and user_id = from_user_id and day = \"%s\""

    for k, user_arr in user_channel.items():
        data[k] = {}
        data[k]["num_task"] = 0
        data[k]["num_user"] = 0
        for i in range(0, len(user_arr), 1000):
            user_to_sql = user_arr[i:i+1000]
            sql_use = sql % (",".join(user_to_sql), day_str)
            rt = mysql_tool.querydb(
                sql_use, logger, "get task count for %d users" % len(user_to_sql))
            if len(rt) > 0 and rt[0][0] is not None:
                data[k]["num_task"] += int(rt[0][0])
                data[k]["num_user"] += int(rt[0][1])
        data["all_channel"]["num_task"] += data[k]["num_task"]
        data["all_channel"]["num_user"] += data[k]["num_user"]

    return data


def get_reading(user_channel={}, nday=1):
    data = {}
    data["all_channel"] = {}
    data["all_channel"]["num_read"] = 0
    data["all_channel"]["num_user"] = 0
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    sql = "select count(user_id), count(distinct user_id) from news_effective_readings where user_id in (%s) and created_at >= \"%s\" and created_at < \"%s\" and effective = 1"

    for k, user_arr in user_channel.items():
        data[k] = {}
        data[k]["num_read"] = 0
        data[k]["num_user"] = 0
        for i in range(0, len(user_arr), 1000):
            user_to_sql = user_arr[i:i+1000]
            sql_use = sql % (",".join(user_to_sql), day1, day2)
            rt = mysql_tool.querydb(
                sql_use, logger, "get reading count for %d users" % len(user_to_sql))
            if len(rt) > 0 and rt[0][0] is not None:
                data[k]["num_read"] += int(rt[0][0])
                data[k]["num_user"] += int(rt[0][1])
        data["all_channel"]["num_read"] += data[k]["num_read"]
        data["all_channel"]["num_user"] += data[k]["num_user"]

    return data


def get_video(user_channel={}, nday=1):
    data = {}
    data["all_channel"] = {}
    data["all_channel"]["num_video"] = 0
    data["all_channel"]["num_user"] = 0
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    sql = "select count(user_id), count(distinct user_id) from video_effective_readings where user_id in (%s) and created_at >= \"%s\" and created_at < \"%s\" and effective = 1"

    for k, user_arr in user_channel.items():
        data[k] = {}
        data[k]["num_video"] = 0
        data[k]["num_user"] = 0
        for i in range(0, len(user_arr), 1000):
            user_to_sql = user_arr[i:i+1000]
            sql_use = sql % (",".join(user_to_sql), day1, day2)
            rt = mysql_tool.querydb(
                sql_use, logger, "get video count for %d users" % len(user_to_sql))
            if len(rt) > 0 and rt[0][0] is not None:
                data[k]["num_video"] += int(rt[0][0])
                data[k]["num_user"] += int(rt[0][1])
        data["all_channel"]["num_video"] += data[k]["num_video"]
        data["all_channel"]["num_user"] += data[k]["num_user"]

    return data


def get_child(user_channel={}, nday=1):
    data = {}
    data["all_channel"] = 0
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    sql = "select count(us.channel) from users as u left join user_statistics as us on (u.id = us.user_id) where u.parent_id in (%s) and u.bind_parent_at >= \"%s\" and u.bind_parent_at < \"%s\""

    for k, user_arr in user_channel.items():
        data[k] = 0
        for i in range(0, len(user_arr), 1000):
            user_to_sql = user_arr[i:i+1000]
            sql_use = sql % (",".join(user_to_sql), day1, day2)
            rt = mysql_tool.querydb(
                sql_use, logger, "get child count for %d users" % len(user_to_sql))
            if len(rt) > 0 and rt[0][0] is not None:
                data[k] += int(rt[0][0])
        data["all_channel"] += data[k]

    return data


def process(nday=1):
    mysql_tool.connectdb()

    device_arr = get_device_arr(nday)
    device_per_channel = get_device_channel(device_arr)
    user_arr = get_user_arr(nday)
    # user_per_channel = get_user_channel_(user_arr) Got user channel info is not same as get_user_channel.
    # get_user_channel which get info from db, should use db info firstly
    user_channel = get_user_channel(user_arr)
    user_channel_stay = copy.deepcopy(user_channel)
    user_channel_stay["all_channel"] = user_arr
    app_stay, app_stay_first = get_app_stay(
        get_query_app_stay(), user_channel_stay, nday)
    task_per_channel = get_task(user_channel, nday=nday)
    reading_per_channel = get_reading(user_channel, nday=nday)
    video_per_channel = get_video(user_channel, nday=nday)
    child_per_channel = get_child(user_channel, nday=nday)

    mysql_tool.closedb()

    for k, v in user_channel_stay.items():
        len_v = len(v)
        data = {}
        data["channel"] = k
        data["@timestamp"] = time_tool.get_someday_es_format(-nday)
        data["num_user"] = len_v
        if k in device_per_channel.keys():
            data["num_device"] = device_per_channel[k]
        if k in task_per_channel.keys() and task_per_channel[k]["num_user"] > 0:
            data["num_task_average"] = round(
                task_per_channel[k]["num_task"] / float(task_per_channel[k]["num_user"]), 2)
        if k in reading_per_channel.keys():
            data["num_read"] = reading_per_channel[k]["num_read"]
            data["num_read_user"] = reading_per_channel[k]["num_user"]
            if reading_per_channel[k]["num_user"] > 0:
                data["num_read_average"] = round(
                    data["num_read"] / float(data["num_read_user"]), 2)
        if k in video_per_channel.keys():
            data["num_video"] = video_per_channel[k]["num_video"]
            data["num_video_user"] = video_per_channel[k]["num_user"]
            if data["num_video_user"] > 0:
                data["num_video_average"] = round(
                    data["num_video"] / float(data["num_video_user"]), 2)
        if k in child_per_channel.keys():
            data["num_child"] = child_per_channel[k]
        if k in app_stay.keys():
            data["app_stay"] = int(app_stay[k])
            if data["app_stay"] > 60:
                data["app_stay_show"] = str(
                    data["app_stay"] / 60) + u"分" + str(data["app_stay"] % 60) + u"秒"
            else:
                data["app_stay_show"] = str(data["app_stay"]) + u"秒"
        if k in app_stay_first.keys():
            data["app_stay_first"] = int(app_stay_first[k])
            if data["app_stay_first"] > 60:
                data["app_stay_first_show"] = str(
                    data["app_stay_first"] / 60) + u"分" + str(data["app_stay_first"] % 60) + u"秒"
            else:
                data["app_stay_first_show"] = str(
                    data["app_stay_first"]) + u"秒"
        url = URL_ELASTICSEARCH_ACTIVE_USER_INFO + "/" + \
            time_tool.get_someday_str(-nday) + "_" + k
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(data), timeout=(10, 20))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request active_user_info index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)


if __name__ == '__main__':
    process()
