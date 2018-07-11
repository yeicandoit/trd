# -*- coding: utf-8 -*-
# 设当日前一天为X, 计算X往前1/2/3/4/5/6/7/15/30天的新增用户/新增用户,
# 是否在X这天有日志, 有即算做X的第1/2/3/4/5/6/7/15/30日留存数据
import requests
import json
import time
import copy
from util import time_tool
from datetime import datetime, timedelta


URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_USER = "http://localhost:9200/user/doc/_search"
URL_ELASTICSEARCH_REMAIN = "http://localhost:9200/remain"
URL_ELASTICSEARCH_REMAIN_ID = "http://localhost:9200/remain/doc/%s"
JSON_HEADER = {"Content-Type": "application/json"}
REMAIN_MAPPING = {
    "properties": {
        "new_user_num": {
            "type": "long"
        },
        "user_remain_rate_1d": {
            "type": "float"
        },
        "user_remain_rate_2d": {
            "type": "float"
        },
        "user_remain_rate_3d": {
            "type": "float"
        },
        "user_remain_rate_4d": {
            "type": "float"
        },
        "user_remain_rate_5d": {
            "type": "float"
        },
        "user_remain_rate_6d": {
            "type": "float"
        },
        "user_remain_rate_7d": {
            "type": "float"
        },
        "user_remain_rate_14d": {
            "type": "float"
        },
        "user_remain_rate_30d": {
            "type": "float"
        },
    }
}


def get_query_user():
    search_data = {
        "size": 0,
        "aggs": {
            "uniq_user": {
                "terms": {  # get the user_id arry by terms,
                    # but the arry size may be smaller than unique user_id size got by cardinality
                    "field": "user_id.keyword",
                    "size": 1000000000
                }
            }
        },
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": 0,
                                "lte": 0,
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
        }
    }

    return search_data


def get_query_user_1():
    search_data = get_query_user()
    search_data["aggs"]["uniq_user"]["terms"]["field"] = "user_id"
    return search_data


def uniq_user_1day(url="", query={}, nday=1):
    start = time_tool.get_weehours_of_someday(nday)
    users = []
    for i in range(24):
        query["query"]["bool"]["must"] = [
            {
                "range": {
                    "@timestamp": {
                        "gte": (start + i * 3600) * 1000,
                        "lte": (start + (i + 1) * 3600) * 1000 - 1,
                        "format": "epoch_millis"
                    }
                }
            }

        ]
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(query), timeout=(30, 60))
        if 200 == r.status_code:
            r_json = r.json()
            # print r_json
            arr_user_id = [user['key']
                           for user in r_json['aggregations']['uniq_user']['buckets']]
            users = list(set(arr_user_id).union(set(users)))
        else:
            print "request applog index failed, status_code:%d, reason:%s" % (
                r.status_code, r.reason)
            return []
    return users


def create_remain_index():
    requests.put(URL_ELASTICSEARCH_REMAIN)
    requests.post(URL_ELASTICSEARCH_REMAIN + "/doc/_mapping",
                  headers=JSON_HEADER, data=json.dumps(REMAIN_MAPPING), timeout=(10, 20))


def set_remain_rate(key, rate, new_user_num, someday):
    remain_rate_key = "user_remain_rate_%dd" % key
    dt = datetime.today() - timedelta(key+1+someday)
    _id = dt.strftime('%Y-%m-%d')
    url = URL_ELASTICSEARCH_REMAIN_ID % _id
    timestamp = dt.isoformat() + "+08:00"

    r = requests.get(url, timeout=(10, 20))
    r_json = r.json()
    if r_json["found"]:
        data = r_json["_source"]
        data[remain_rate_key] = rate
        data["new_user_num"] = new_user_num
        requests.post(url, headers=JSON_HEADER,
                      data=json.dumps(data), timeout=(10, 20))
    else:
        data = {
            "@timestamp": timestamp,
            remain_rate_key: rate,
            "new_user_num": new_user_num
        }
        requests.post(url, headers=JSON_HEADER,
                      data=json.dumps(data), timeout=(10, 20))


def update_rate(d=0):
    yud = uniq_user_1day(URL_ELASTICSEARCH_APPLOG, get_query_user(), -(d+1))
    nd1day = uniq_user_1day(URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+2))
    nd2day = uniq_user_1day(URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+3))
    nd3day = uniq_user_1day(URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+4))
    nd4day = uniq_user_1day(URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+5))
    nd5day = uniq_user_1day(URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+6))
    nd6day = uniq_user_1day(URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+7))
    nd7day = uniq_user_1day(URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+8))
    nd14day = uniq_user_1day(URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+15))
    nd30day = uniq_user_1day(URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+31))

    for key, nd in [(1, nd1day), (2, nd2day), (3, nd3day), (4, nd4day), (5, nd5day), (6, nd6day), (7, nd7day), (14, nd14day), (30, nd30day)]:
        ret = list(set(nd).intersection(set(yud)))
        if 0 == len(nd):
            continue
        rate = round(len(ret)/float(len(nd)), 4)
        print key, rate, len(nd)
        set_remain_rate(key, rate, len(nd), d)


if __name__ == '__main__':
    # create_remain_index()
    update_rate()
