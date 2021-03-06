# coding=utf-8
import requests
import json
import time
import copy
from util import time_tool
from datetime import datetime, timedelta


URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_APP_STAY = "http://localhost:9200/app-stay/doc"
JSON_HEADER = {"Content-Type": "application/json"}

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/app-stay/doc/_mapping -d '{"properties": {"app_stay":{"type":"long"},"app_stay_first":{"type":"long"},"@timestamp":{"type":"date"}, "app_stay_show":{"type":"keyword"}, "app_stay_first_show":{"type":"keyword"}}}'


def get_query_use_time(nday):
    yesterday = time_tool.get_weehours_of_someday(-nday)
    search_data = {
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
                        "range": {
                            "@timestamp": {
                                "gte": yesterday * 1000,
                                "lte": (yesterday + 86400) * 1000 - 1,
                                "format": "epoch_millis"
                            }
                        }
                    },
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
                ]
            }
        }
    }

    return search_data


def get_query_device(nday):
    yesterday = time_tool.get_weehours_of_someday(-nday)
    search_data = {
        "size": 0,
        "aggs": {
            "uniq_device": {
                "cardinality": {
                    "field": "device_id.keyword",
                }
            }
        },
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": yesterday * 1000,
                                "lte": (yesterday + 86400) * 1000 - 1,
                                "format": "epoch_millis"
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            "key.keyword": {
                                "query": "app_common_use_time"
                            }
                        }
                    }
                ]
            }
        }
    }

    return search_data


def sum_time_is_first(query={}):
    query_is_first = {
        "match_phrase": {
            "is_first": {
                "query": "true"
            }
        }
    }
    query["query"]["bool"]["must"].append(query_is_first)
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        # print r_json
        return r_json["aggregations"]["sum_time"]["value"]
    else:
        print "request applog index failed, status_code:%d, reason:%s" % (
            r.status_code, r.reason)
        return 1


def uniq_device_is_first(query={}):
    query_is_first = {
        "match_phrase": {
            "is_first": {
                "query": "true"
            }
        }
    }
    query["query"]["bool"]["must"].append(query_is_first)
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        # print r_json
        return r_json["aggregations"]["uniq_device"]["value"]
    else:
        print "request applog index failed, status_code:%d, reason:%s" % (
            r.status_code, r.reason)
        return 1


def sum_time(query={}):
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        # print r_json
        return r_json["aggregations"]["sum_time"]["value"]
    else:
        print "request applog index failed, status_code:%d, reason:%s" % (
            r.status_code, r.reason)
        return []


def uniq_device(query={}):
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        # print r_json
        return r_json["aggregations"]["uniq_device"]["value"]
    else:
        print "request applog index failed, status_code:%d, reason:%s" % (
            r.status_code, r.reason)
        return 1


def update_app_stay(app_stay_first=1, app_stay=1, someday=1):
    dt = datetime.today() - timedelta(someday)
    _id = dt.strftime('%Y-%m-%d')
    app_stay_data = {
        "@timestamp": dt.isoformat() + "+08:00",
        "app_stay": app_stay,
        "app_stay_show": str(app_stay/60) + u"分" + str(app_stay % 60) + u"秒" if app_stay > 60 else str(app_stay) + u"秒",
        "app_stay_first": app_stay_first,
        "app_stay_first_show": str(app_stay_first/60) + u"分" + str(app_stay_first % 60) + u"秒" if app_stay_first > 60 else str(app_stay_first) + u"秒"
    }
    url = URL_ELASTICSEARCH_APP_STAY + "/" + _id
    print app_stay_data
    requests.post(url, headers=JSON_HEADER,
                  data=json.dumps(app_stay_data), timeout=(10, 20))


if __name__ == '__main__':
    nday = 1
    query_sum_time = get_query_use_time(nday)
    query_device = get_query_device(nday)
    stf = sum_time_is_first(copy.deepcopy(query_sum_time))
    st = sum_time(copy.deepcopy(query_sum_time))
    udf = uniq_device_is_first(copy.deepcopy(query_device))
    ud = uniq_device(copy.deepcopy(query_device))
    update_app_stay(int(stf/udf), int(st/ud), nday)
