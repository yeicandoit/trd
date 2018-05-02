# -*- coding: utf-8 -*-
# 设当日前一天为X, 计算X往前1/2/3/4/5/6/7/15/30天的新增用户/新增设备,
# 是否在X这天有日志, 有即算做X的第1/2/3/4/5/6/7/15/30日留存数据
import requests
import json
import time
import copy
from util import time_tool
from datetime import datetime, timedelta


URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_DEVICE = "http://localhost:9200/device/doc/_search"
URL_ELASTICSEARCH_REMAIN = "http://localhost:9200/remain"
URL_ELASTICSEARCH_REMAIN_ID = "http://localhost:9200/remain/doc/%s"
JSON_HEADER = {"Content-Type": "application/json"}
REMAIN_MAPPING = {
    "properties": {
        "@timestamp": {
            "type": "date"
        },
        "new_device_num": {
            "type": "long"
        },
        "remain_rate_1d": {
            "type": "float"
        },
        "remain_rate_2d": {
            "type": "float"
        },
        "remain_rate_3d": {
            "type": "float"
        },
        "remain_rate_4d": {
            "type": "float"
        },
        "remain_rate_5d": {
            "type": "float"
        },
        "remain_rate_6d": {
            "type": "float"
        },
        "remain_rate_7d": {
            "type": "float"
        },
        "remain_rate_14d": {
            "type": "float"
        },
        "remain_rate_30d": {
            "type": "float"
        },
    }
}


def get_query_device():
    search_data = {
        "size": 0,
        "aggs": {
            "uniq_device": {
                "terms": {  # get the device_id arry by terms,
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


def uniq_device_1day(url="", query={}, nday=1):
    start = time_tool.get_weehours_of_someday(nday)
    devices = []
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
                          data=json.dumps(query), timeout=(30, 300))
        if 200 == r.status_code:
            r_json = r.json()
            # print r_json
            arr_device_id = [device['key']
                             for device in r_json['aggregations']['uniq_device']['buckets']]
            devices = list(set(arr_device_id).union(set(devices)))
        else:
            print "request applog index failed, status_code:%d, reason:%s" % (
                r.status_code, r.reason)
            return []
    return devices


def create_remain_index():
    requests.put(URL_ELASTICSEARCH_REMAIN)
    requests.post(URL_ELASTICSEARCH_REMAIN + "/doc/_mapping",
                  headers=JSON_HEADER, data=json.dumps(REMAIN_MAPPING), timeout=(10, 20))


def set_remain_rate(key, rate, new_device_num):
    remain_rate_key = "remain_rate_%dd" % key
    dt = datetime.today() - timedelta(key+1)
    _id = dt.strftime('%Y-%m-%d')
    url = URL_ELASTICSEARCH_REMAIN_ID % _id
    timestamp = dt.isoformat() + "+08:00"

    r = requests.get(url, timeout=(10, 20))
    r_json = r.json()
    if r_json["found"]:
        data = r_json["_source"]
        data[remain_rate_key] = rate
        data["new_device_num"] = new_device_num
        requests.post(url, headers=JSON_HEADER,
                      data=json.dumps(data), timeout=(10, 20))
    else:
        data = {
            "@timestamp": timestamp,
            remain_rate_key: rate,
            "new_device_num": new_device_num
        }
        requests.post(url, headers=JSON_HEADER,
                      data=json.dumps(data), timeout=(10, 20))


if __name__ == '__main__':
    # create_remain_index()
    # set_remain_rate(5, 0.19, 68678)
    yud = uniq_device_1day(URL_ELASTICSEARCH_APPLOG, get_query_device(), -1)
    nd1day = uniq_device_1day(URL_ELASTICSEARCH_DEVICE, get_query_device(), -2)
    nd2day = uniq_device_1day(URL_ELASTICSEARCH_DEVICE, get_query_device(), -3)
    nd3day = uniq_device_1day(URL_ELASTICSEARCH_DEVICE, get_query_device(), -4)
    nd4day = uniq_device_1day(URL_ELASTICSEARCH_DEVICE, get_query_device(), -5)
    nd5day = uniq_device_1day(URL_ELASTICSEARCH_DEVICE, get_query_device(), -6)
    nd6day = uniq_device_1day(URL_ELASTICSEARCH_DEVICE, get_query_device(), -7)
    nd7day = uniq_device_1day(URL_ELASTICSEARCH_DEVICE, get_query_device(), -8)
    nd14day = uniq_device_1day(URL_ELASTICSEARCH_DEVICE, get_query_device(), -15)
    nd30day = uniq_device_1day(URL_ELASTICSEARCH_DEVICE, get_query_device(), -31)

    for key, nd in [(1, nd1day), (2, nd2day), (3, nd3day), (4, nd4day), (5, nd5day), (6, nd6day), (7, nd7day), (14, nd14day), (30, nd30day)]:
        ret = list(set(nd).intersection(set(yud)))
        if 0 == len(nd):
            continue
        rate = len(ret)/float(len(nd))
        print key, rate, len(nd), len(ret), len(yud)
        set_remain_rate(key, rate, len(nd))
