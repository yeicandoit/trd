# -*- coding: utf-8 -*-
# 设当日前一天为X, 计算X往前1/2/3/4/5/6/7/15/30天的各渠道新增用户/新增设备,
# 是否在X这天有日志, 有即算做X的第1/2/3/4/5/6/7/15/30日各渠道留存数据


import json
import requests
from datetime import datetime, timedelta
import cal_user_remain as cr
from util import time_tool


def get_query_user():
    search_data = {
        "size": 0,
        "aggs": {
            "uniq_channel": {
                "terms": {
                    "field": "channel.keyword",
                    "size": 1000000000
                },
                "aggs": {
                    "uniq_user": {
                        "terms": {
                            "field": "user_id.keyword",
                            "size": 1000000000
                        }
                    }
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
    search_data["aggs"]["uniq_channel"]["terms"]["field"] = "channel"
    search_data["aggs"]["uniq_channel"]["aggs"]["uniq_user"]["terms"]["field"] = "user_id"
    return search_data


def uniq_channel_user_1day(url="", query={}, nday=1):
    start = time_tool.get_weehours_of_someday(nday)
    hash_channel_user = {}
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
        r = requests.post(url, headers=cr.JSON_HEADER,
                          data=json.dumps(query), timeout=(30, 300))
        if 200 == r.status_code:
            r_json = r.json()
            for channel in r_json['aggregations']['uniq_channel']['buckets']:
                channel_key = "unkown" if '' == channel['key'] else channel['key']
                users = [user['key']
                           for user in channel['uniq_user']['buckets']]
                if channel_key in hash_channel_user.keys():
                    hash_channel_user[channel_key] = list(
                        set(hash_channel_user[channel_key]).union(set(users)))
                else:
                    hash_channel_user[channel_key] = users

        else:
            print "request applog index failed, status_code:%d, reason:%s" % (
                r.status_code, r.reason)
            return {}

    return hash_channel_user


def set_channel_user_remain_rate(key, channel, rate, new_user_num, someday):
    remain_rate_key = "user_remain_rate_%dd" % key
    dt = datetime.today() - timedelta(key+1+someday)
    _id = dt.strftime('%Y-%m-%d')
    url = cr.URL_ELASTICSEARCH_REMAIN_ID % _id + "_" + channel
    timestamp = dt.isoformat() + "+08:00"

    r = requests.get(url, timeout=(10, 120))
    r_json = r.json()
    if r_json["found"]:
        data = r_json["_source"]
        data[remain_rate_key] = rate
        data["new_user_num"] = new_user_num
        data["channel"] = channel
        requests.post(url, headers=cr.JSON_HEADER,
                      data=json.dumps(data), timeout=(10, 20))
    else:
        data = {
            "@timestamp": timestamp,
            remain_rate_key: rate,
            "new_user_num": new_user_num,
            "channel": channel
        }
        requests.post(url, headers=cr.JSON_HEADER,
                      data=json.dumps(data), timeout=(10, 20))


def set_remain_rate(d=0):
    yud = cr.uniq_user_1day(
        cr.URL_ELASTICSEARCH_APPLOG, cr.get_query_user(), -(d+1))
    nd1day = uniq_channel_user_1day(
        cr.URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+2))
    nd2day = uniq_channel_user_1day(
        cr.URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+3))
    nd3day = uniq_channel_user_1day(
        cr.URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+4))
    nd4day = uniq_channel_user_1day(
        cr.URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+5))
    nd5day = uniq_channel_user_1day(
        cr.URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+6))
    nd6day = uniq_channel_user_1day(
        cr.URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+7))
    nd7day = uniq_channel_user_1day(
        cr.URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+8))
    nd14day = uniq_channel_user_1day(
        cr.URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+15))
    nd30day = uniq_channel_user_1day(
        cr.URL_ELASTICSEARCH_USER, get_query_user_1(), -(d+31))

    for key, nd in [(1, nd1day), (2, nd2day), (3, nd3day), (4, nd4day), (5, nd5day), (6, nd6day), (7, nd7day), (14, nd14day), (30, nd30day)]:
        for channel in nd:
            if 0 == len(nd[channel]):
                print "nd%dday[%s]' lenght is zero" % (key, channel)
                continue
            ret = list(set(nd[channel]).intersection(set(yud)))
            rate = round(len(ret)/float(len(nd[channel])), 2)
            print key, rate, len(ret), len(
                nd[channel]), len(yud), channel
            set_channel_user_remain_rate(
                key, channel, rate, len(nd[channel]), d)


if __name__ == '__main__':
    set_remain_rate()
