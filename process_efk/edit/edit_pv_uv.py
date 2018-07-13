# coding=utf-8
import requests
import json
from util import mysql_tool, time_tool, log_tool
from conf import config
import define


logger = log_tool.logger


def get_news(nday=1, key="key.keyword", value="app_news_show_all_button_click"):
    start = time_tool.get_weehours_of_someday(-nday)
    query = {
        "size": 0,
        "query": {
            "bool": {
                "must_not": [
                    {
                        "match_phrase": {
                            "news_id": -1
                        }
                    }
                ],
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start * 1000,
                                "lte": (start + 86400) * 1000 - 1,
                                "format": "epoch_millis"
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            key: value
                        }
                    }
                ]
            }
        },
        "aggs": {
            "news_id_arr": {
                "terms": {
                    "field": "news_id",
                    "size": 100000000
                }
            }
        }
    }

    news_id_arr = []
    r = requests.post(config.URL_APPLOG_SEARCH, headers=config.JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 120))
    if 200 == r.status_code:
        r_json = r.json()
        news_id_arr = [n["key"]
                       for n in r_json["aggregations"]["news_id_arr"]["buckets"]]
    else:
        logger.error(
            "request applog index failed, status_code:%d, reason:%s", r.status_code, r.reason)

    return news_id_arr


# get pv, uv for total
def get_puv4total(nday=1, key="key.keyword", value="app_news_show_all_button_click"):
    pv = 0
    uv = 0
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
                    },
                    {
                        "match_phrase": {
                            key: value
                        }
                    }
                ]
            }
        },
        "aggs": {
            "count": {
                "cardinality": {
                    "field": "user_id.keyword"
                }
            }
        }
    }
    r = requests.post(config.URL_APPLOG_SEARCH, headers=config.JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 120))
    if 200 == r.status_code:
        r_json = r.json()
        pv = r_json["hits"]["total"]
        uv = r_json["aggregations"]["count"]["value"]
    else:
        logger.error(
            "request applog index for news pv, uv failed, status_code:%d, reason:%s", r.status_code, r.reason)
    return pv, uv


# get pv, uv
def get_puv4news(nday=1, news_id_arr=[], key="key.keyword", value="app_news_show_all_button_click"):
    pv = 0
    uv = 0
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
                    },
                    {
                        "match_phrase": {
                            key: value
                        }
                    }
                ],
                "filter": {
                    "terms": {
                        "news_id": news_id_arr
                    }
                }
            }
        },
        "aggs": {
            "count": {
                "cardinality": {
                    "field": "user_id.keyword"
                }
            }
        }
    }
    r = requests.post(config.URL_APPLOG_SEARCH, headers=config.JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 120))
    if 200 == r.status_code:
        r_json = r.json()
        pv = r_json["hits"]["total"]
        uv = r_json["aggregations"]["count"]["value"]
    else:
        logger.error(
            "request applog index for news pv, uv failed, status_code:%d, reason:%s", r.status_code, r.reason)
    return pv, uv


def get_list_news_category(list_news={}):
    for table, category_id in [("news_hot_lists", define.NEWS_HOT_LISTS), 
                               ("news_hot_seven_day_lists", define.NEWS_HOT_SEVEN), 
                               ("news_hot_total_lists", define.NEWS_HOT_TOTAL)]:
        list_news[category_id] = []
        sql = "select news_id from %s" % table
        rt = mysql_tool.querydb(sql, logger, sql)
        for v in rt:
            list_news[category_id].append(int(v[0]))
    return list_news


def get_news_category(nday=1, news_id_arr=[]):
    day = time_tool.get_someday_str(-nday+1)
    list_news = {}
    sql = "select id, category_id from news where id in (%s)"
    for i in range(0, len(news_id_arr), 1000):
        id_arr = [str(nid) for nid in news_id_arr[i:i+1000]]
        sql_use = sql % (",".join(id_arr))
        rt = mysql_tool.querydb( 
                sql_use, logger, "select id, category_id from news for %d news" % len(id_arr))
        for v in rt:
            news_id = int(v[0])
            category_id = int(v[1])
            if category_id in list_news.keys():
                list_news[category_id].append(news_id)
            else:
                list_news[category_id] = [news_id]
    list_news[define.HOT_NEWS_CATEGORY_ID] = []
    sql = "select id from news where hot_at < \"" + day + "\" and id in (%s)"
    for i in range(0, len(news_id_arr), 1000):
        id_arr = [str(nid) for nid in news_id_arr[i:i+1000]]
        sql_use = sql % (",".join(id_arr))
        rt = mysql_tool.querydb(
            sql_use, logger, "select hot id from news for %d news" % len(id_arr))
        for v in rt:
            list_news[define.HOT_NEWS_CATEGORY_ID].append(int(v[0]))

    list_news = get_list_news_category(list_news)
    return list_news
            

def process(nday=1):
    mysql_tool.connectdb(host="47.96.238.205", database="taozuiredian-news")
    data = {}
    for key, value, k in [("key.keyword", "app_news_show_all_button_click", "show_all")]:
        news_id_arr = get_news(nday=nday, key=key, value=value)
        news_category = get_news_category(nday=nday, news_id_arr=news_id_arr)
        for category_id, news_arr in news_category.items():
            if category_id not in data.keys():
                data[category_id] = {}
            pv, uv = get_puv4news(nday=nday, news_id_arr=news_arr, key=key, value=value)
            data[category_id][k+"_pv"] = pv
            data[category_id][k+"_uv"] = uv
            if uv > 0 and k == "show_all":
                data[category_id][k+"_pv_uv"] = round(float(pv)/uv, 2)
        pv, uv = get_puv4total(nday=nday, key=key, value=value)
        if define.TOTAL_ID not in data.keys():
            data[define.TOTAL_ID] = {}
        data[define.TOTAL_ID][k+"_pv"] = pv
        data[define.TOTAL_ID][k+"_uv"] = uv
        if uv > 0 and k == "show_all":
            data[define.TOTAL_ID][k+"_pv_uv"] = round(float(pv)/uv, 2)
    mysql_tool.closedb()
    
    return data


if __name__ == '__main__':
    process()
