# coding=utf-8
import requests
import json
import logging.config
import active_user_info
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_USER = "http://localhost:9200/user/doc/_search"
URL_ELASTICSEARCH_EDIT_USER_INFO = "http://localhost:9200/edit_user_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}
HOT_NEWS_CATEGORY_ID = 99990
HOT_VIDEO_CATEGORY_ID = 99991
HOT_SHOW = u"热点"

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/edit_user_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "user_type":{"type":"keyword"}, "channel":{"type":"keyword"}, "channel_name":{"type":"keyword"}, "category_id":{"type":"long"}, "category_name":{"type":"keyword"}, "effective_reading":{"type":"long"}, "comments_count":{"type":"long"}}}'


def get_new_user_arr(nday=1):
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
                ]
            }
        },
        "aggs": {
            "user_arr": {
                "terms": {
                    "field": "user_id",
                    "size": 10000000
                }
            }
        }
    }

    data = []
    r = requests.post(URL_ELASTICSEARCH_USER, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        data = [user['key']
                for user in r_json['aggregations']['user_arr']['buckets']]
    else:
        logger.error("request user index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return data


def get_edit_news_info(user_arr=[], user_type="new", nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)

    # 获取资讯名称
    news_categories = {}
    sql = "select id, name from news_categories"
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        news_categories[int(v[0])] = v[1]

    # 获取有效阅读
    data = {}
    sql = "select n.category_id, count(*) from news_effective_readings as ner join news as n on (ner.news_id = n.id) where ner.user_id in (%s) and ner.created_at >= \"%s\" and ner.created_at < \"%s\" and ner.effective != 0 group by n.category_id"
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql), day1, day2)
        rt = mysql_tool.querydb(
            sql_use, logger, "select news_effective_readings for %d users" % len(user_to_sql))
        for v in rt:
            k = int(v[0])
            if k in data.keys():
                data[k]["effective_reading"] += int(v[1])
            else:
                data[k] = {}
                data[k]["user_type"] = user_type
                data[k]['channel_name'] = u"资讯"
                data[k]['channel'] = "news"
                data[k]['category_id'] = k
                data[k]["effective_reading"] = int(v[1])
                if k in news_categories.keys():
                    data[k]['category_name'] = news_categories[k]

    # 获取评论
    sql = "select n.category_id, count(*) from news_comments as nc join news as n on (nc.news_id = n.id) where nc.user_id in (%s) and nc.created_at >= \"%s\" and nc.created_at < \"%s\" group by n.category_id"
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql), day1, day2)
        rt = mysql_tool.querydb(
            sql_use, logger, "select news_comments for %d users" % len(user_to_sql))
        for v in rt:
            k = int(v[0])
            if k in data.keys():
                if "comments_count" in data[k].keys():
                    data[k]["comments_count"] += int(v[1])
                else:
                    data[k]["comments_count"] = int(v[1])
            else:
                data[k] = {}
                data[k]["user_type"] = user_type
                data[k]['channel_name'] = u"资讯"
                data[k]['channel'] = "news"
                data[k]['category_id'] = k
                data[k]["comments_count"] = int(v[1])
                if k in news_categories.keys():
                    data[k]['category_name'] = news_categories[k]

    return data


def get_edit_video_info(user_arr=[], user_type="new", nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)

    # 获取视频名称
    video_categories = {}
    sql = "select id, name from video_categories"
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        video_categories[int(v[0])] = v[1]

    # 获取有效阅读
    video_effective_reading = {}
    sql = "select v.category_id, count(*) from video_effective_readings as ver join videos as v on (ver.video_id = v.id) where ver.user_id in (%s) and ver.created_at >= \"%s\" and ver.created_at < \"%s\" and ver.effective != 0 group by v.category_id"
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql), day1, day2)
        rt = mysql_tool.querydb(
            sql_use, logger, "select video_effective_reading for %d users" % len(user_to_sql))
        for v in rt:
            k = int(v[0])
            if k in data.keys():
                data[k]["effective_reading"] += int(v[1])
            else:
                data[k] = {}
                data[k]["user_type"] = user_type
                data[k]['channel_name'] = u"视频"
                data[k]['channel'] = "video"
                data[k]['category_id'] = k
                data[k]["effective_reading"] = int(v[1])
                if k in video_categories.keys():
                    data[k]['category_name'] = video_categories[k]
    return data


def process(nday=1):
    active_user_arr = active_user_info.get_user_arr(nday)
    new_user_arr = get_new_user_arr(nday)
    active_not_new_user_arr = list(set(active_user_arr) - set(new_user_arr))

    for user_arr, user_type in [(active_not_new_user_arr, "not_new"), (new_user_arr, "new")]:
        mysql_tool.connectdb()
        mysql_tool.querydb("SET NAMES utf8mb4")
        data_news = get_edit_news_info(user_arr, user_type, nday)
        data_video = get_edit_video_info(user_arr, user_type, nday)
        mysql_tool.closedb()
        for data in [data_news, data_video]:
            for k, v in data.items():
                v['@timestamp'] = time_tool.get_someday_es_format(-nday)
                _id = time_tool.get_someday_str(-nday)
                url = URL_ELASTICSEARCH_EDIT_USER_INFO + \
                    "/" + _id + "_" + v['user_type'] + \
                    "_" + v['channel'] + "_" + str(k)
                r = requests.post(url, headers=JSON_HEADER,
                                  data=json.dumps(v), timeout=(30, 120))
                if 200 != r.status_code and 201 != r.status_code:
                    logger.error("request edit index failed, status_code:%d, reason:%s, %s, %s",
                                 r.status_code, json.dumps(r.json()), url, json.dumps(v))


if __name__ == '__main__':
    process()
