# -*- coding: utf-8 -*-
# 设当日前一天为X, 计算X往前1/2/3/4/5/6/7/15/30天的新增用户/新增设备,
# 是否在X这天有日志, 有即算做X的第1/2/3/4/5/6/7/15/30日留存数据
import requests
import json
import time
import copy
from datetime import datetime, timedelta


URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_DEVICE = "http://localhost:9200/device/doc/_search"
JSON_HEADER = {"Content-Type": "application/json"}


def get_query_device(ndays):
    now_time = int(time.time())
    day_time = now_time - now_time % 86400 + time.timezone
    ndays_ago = day_time - ndays * 86400
    search_data = {
        "size": 0,
        "aggs": {
            "uniq_device": {
                "terms": { # get the device_id arry by terms, 
                           # but the arry size may be smaller than unique device_id size got by cardinality
                    "field": "device_id.keyword",
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
                                "gte": ndays_ago * 1000,
                                "lte": (ndays_ago + 86400) * 1000 - 1,
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
        }
    }

    return search_data


def uniq_device(url="", query={}):
    r = requests.post(url, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        # print r_json
        arr_device_id = [device['key']
                         for device in r_json['aggregations']['uniq_device']['buckets']]
        return arr_device_id
    else:
        print "request applog index failed, status_code:%d, reason:%s" % (
            r.status_code, r.reason)
        return 1


def update_remain(app_stay_first=1, app_stay=1):
    app_stay_data = {
        "@timestamp": datetime.today().isoformat() + "+08:00",
        "app_stay": app_stay,
        "app_stay_first": app_stay_first
    }
    requests.post(URL_ELASTICSEARCH_APP_STAY, headers=JSON_HEADER,
                  data=json.dumps(app_stay_data), timeout=(10, 20))


if __name__ == '__main__':
    yud = uniq_device(URL_ELASTICSEARCH_APPLOG, get_query_device(1))
    nd1day = uniq_device(URL_ELASTICSEARCH_DEVICE, get_query_device(2))
    nd2day = uniq_device(URL_ELASTICSEARCH_DEVICE, get_query_device(3))
    nd3day = uniq_device(URL_ELASTICSEARCH_DEVICE, get_query_device(4))
    nd4day = uniq_device(URL_ELASTICSEARCH_DEVICE, get_query_device(5))
    nd5day = uniq_device(URL_ELASTICSEARCH_DEVICE, get_query_device(6))
    nd6day = uniq_device(URL_ELASTICSEARCH_DEVICE, get_query_device(7))
    nd7day = uniq_device(URL_ELASTICSEARCH_DEVICE, get_query_device(8))
    nd15day = uniq_device(URL_ELASTICSEARCH_DEVICE, get_query_device(16))
    nd30day = uniq_device(URL_ELASTICSEARCH_DEVICE, get_query_device(31))

    for nd in [nd1day, nd2day, nd3day, nd4day, nd5day, nd6day, nd7day, nd15day, nd30day]:
        ret = list(set(nd).intersection(set(yud)))
        if 0 == len(nd):
            continue
        print len(ret)/float(len(nd))
