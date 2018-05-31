import requests
import json
import logging.config
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_USER = "http://localhost:9200/user/doc/_search"
URL_ELASTICSEARCH_DEVICE = "http://localhost:9200/device/doc/_search"
URL_ELASTICSEARCH_NEW_USER_INFO = "http://localhost:9200/new_user_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/new_user_info/doc/_mapping -d '{"properties": {"channel":{"type":"keyword"}, "@timestamp":{"type":"date"}, "num_device":{"type":"long"},"num_user":{"type":"long"}, "num_task":{"type":"long"}, "num_task_average":{"type":"float"}, "num_read":{"type":"long"}, "num_video":{"type":"long"}, "num_read_average":{"type":"float"}, "app_stay_first":{"type":"long"}, "app_stay":{"type":"long"}, "num_child":{"type":"long"}}}'


def get_query_per_channel():
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": []
            }
        },
        "aggs": {
            "per_channel": {
                "terms": {
                    "field": "channel",
                    "size": 100000
                }
            }
        }
    }
    return query


def get_query_user():
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": []
            }
        },
        "aggs": {
            "per_channel": {
                "terms": {
                    "field": "channel",
                    "size": 100000
                },
                "aggs": {
                    "user_id": {
                        "terms": {
                            "field": "user_id",
                            "size": 100000000
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


def get_new_user(query={}, nday=1):
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

    r = requests.post(URL_ELASTICSEARCH_USER, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(10, 20))
    if 200 == r.status_code:
        r_json = r.json()
        for v in r_json['aggregations']['per_channel']['buckets']:
            data[v['key']] = v['doc_count']
        logger.debug(data)
    else:
        logger.error("request user index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return data


def get_new_device(query={}, nday=1):
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

    r = requests.post(URL_ELASTICSEARCH_DEVICE, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(10, 20))
    if 200 == r.status_code:
        r_json = r.json()
        for v in r_json['aggregations']['per_channel']['buckets']:
            data[v['key']] = v['doc_count']
        logger.debug(data)
    else:
        logger.error("request device index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return data


def get_app_stay(query={}, query_user={}, nday=1):
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

    query_user["query"]["bool"]["must"] = [time_range]
    r = requests.post(URL_ELASTICSEARCH_USER, headers=JSON_HEADER,
                      data=json.dumps(query_user), timeout=(30, 60))
    user_channel = {}
    if 200 == r.status_code:
        r_json = r.json()
        for v in r_json['aggregations']['per_channel']['buckets']:
            user_channel[v['key']] = [user_id['key']
                                      for user_id in v['user_id']['buckets']]
    else:
        logger.error("request user index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)

    query["query"]["bool"]["must"].append(time_range)
    for k, v in user_channel.items():
        query["query"]["bool"]["filter"]["terms"]["user_id.keyword"] = v
        r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                          data=json.dumps(query), timeout=(30, 60))
        if 200 == r.status_code:
            r_json = r.json()
            if len(v) > 0:
                app_stay[k] = r_json["aggregations"]["sum_time"]["value"] / \
                    len(v)
        else:
            logger.error("request user index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)
    query_is_first = {
        "match_phrase": {
            "is_first": {
                "query": "true"
            }
        }
    }
    query["query"]["bool"]["must"].append(query_is_first)
    for k, v in user_channel.items():
        query["query"]["bool"]["filter"]["terms"]["user_id.keyword"] = v
        r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                          data=json.dumps(query), timeout=(30, 60))
        if 200 == r.status_code:
            r_json = r.json()
            if len(v) > 0:
                app_stay_first[k] = r_json["aggregations"]["sum_time"]["value"] / \
                    len(v)
        else:
            logger.error("request user index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)
    return app_stay, app_stay_first


def get_task(nday=1):
    data = {}
    sql = "select us.channel as channel, sum(ut.today_count) as count from users as u left join user_statistics as us on (u.id = us.user_id) right join user_task_day_records as ut on (u.id = ut.user_id and u.id = ut.from_user_id) where u.registered_at >= \"%s\" and u.registered_at < \"%s\" and ut.day = \"%s\" group by us.channel" % (
        time_tool.get_someday_str(-nday), time_tool.get_someday_str(-nday+1), time_tool.get_someday_str(-nday))
    rt = mysql_tool.querydb(sql)
    for v in rt:
        data[v[0]] = int(v[1])
    logger.debug(data)
    return data


def get_reading(nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    sql = "select us.channel as channel, count(nr.user_id) as count from users as u left join user_statistics as us on (u.id = us.user_id) right join news_effective_readings as nr on (u.id = nr.user_id) where u.registered_at >= \"%s\" and u.registered_at < \"%s\" and nr.created_at >= \"%s\" and nr.created_at < \"%s\" and nr.effective = 1 group by us.channel" % (day1, day2, day1, day2)
    rt = mysql_tool.querydb(sql)
    for v in rt:
        data[v[0]] = int(v[1])
    logger.debug(data)
    return data


def get_video(nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    sql = "select us.channel as channel, count(vr.user_id) as count from users as u left join user_statistics as us on (u.id = us.user_id) right join video_effective_readings as vr on (u.id = vr.user_id) where u.registered_at >= \"%s\" and u.registered_at < \"%s\" and vr.created_at >= \"%s\" and vr.created_at < \"%s\" and vr.effective = 1 group by us.channel" % (day1, day2, day1, day2)
    rt = mysql_tool.querydb(sql)
    for v in rt:
        data[v[0]] = int(v[1])
    logger.debug(data)
    return data


def get_child(nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    sql = "select us.channel, count(us.channel) from users as u left join user_statistics as us on (u.id = us.user_id) where u.registered_at >= \"%s\" and u.registered_at < \"%s\" and  u.bind_parent_at >= \"%s\" and u.bind_parent_at < \"%s\" group by us.channel" % (
        day1, day2, day1, day2)
    rt = mysql_tool.querydb(sql)
    for v in rt:
        data[v[0]] = int(v[1])
    return data


def process(nday=1):
    user_per_channel = get_new_user(query=get_query_per_channel(), nday=nday)
    device_per_channel = get_new_device(
        query=get_query_per_channel(), nday=nday)
    app_stay, app_stay_first = get_app_stay(get_query_app_stay(), get_query_user(), nday)
    mysql_tool.connectdb()
    task_per_channel = get_task(nday=nday)
    reading_per_channel = get_reading(nday=nday)
    video_per_channel = get_video(nday=nday)
    child_per_channel = get_child(nday=nday)
    mysql_tool.closedb()

    for k, v in user_per_channel.items():
        data = {}
        data["channel"] = k
        data["@timestamp"] = time_tool.get_someday_es_format(-nday)
        data["num_user"] = v
        if k in device_per_channel.keys():
            data["num_device"] = device_per_channel[k]
        if k in task_per_channel.keys():
            data["num_task"] = task_per_channel[k]
            if v > 0:
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
        url = URL_ELASTICSEARCH_NEW_USER_INFO + "/" + \
            time_tool.get_someday_str(-nday) + "_" + k
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(data), timeout=(10, 20))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request new_user_info index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)


if __name__ == '__main__':
    process()
