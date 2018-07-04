# coding=utf-8
import requests
import json
import logging.config
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_USER = "http://localhost:9200/user/doc/_search"
URL_ELASTICSEARCH_EDIT_USER_INFO = "http://localhost:9200/edit_user_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/edit_user_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "user_type":{"type":"keyword"}, "channel":{"type":"keyword"}, "channel_name":{"type":"keyword"}, "category_id":{"type":"long"}, "category_name":{"type":"keyword"}, "effective_reading":{"type":"long"}, "comments_count":{"type":"long"}, "like_count":{"type":"long"}, "zan_count":{"type":"long"}}}'


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
    ner = {}
    sob = mysql_tool.sql_obj()
    sql = "select news_id, count(*) from news_effective_readings where user_id in (%s) and created_at >= \"%s\" and created_at < \"%s\" and effective != 0 group by news_id"
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql), day1, day2)
        rt = sob.querydb(
            sql_use, logger, "select news_effective_readings for %d users" % len(user_to_sql))
        for v in rt:
            news_id = int(v[0])
            if news_id in ner.keys():
                ner[news_id] += int(v[1])
            else:
                ner[news_id] = int(v[1])
    sob.closedb()
    news_id_arr = [str(news_id) for news_id in ner.keys()]
    # 获取普通类目有效阅读
    sql = "select id, category_id from news where id in (%s)"
    for i in range(0, len(news_id_arr), 1000):
        id_arr = news_id_arr[i:i+1000]
        sql_use = sql % (",".join(id_arr))
        rt = mysql_tool.querydb(
            sql_use, logger, "select id, category_id from news for %d news" % len(id_arr))
        for v in rt:
            news_id = int(v[0])
            category_id = int(v[1])
            if category_id in data.keys():
                data[category_id]["effective_reading"] += ner[news_id]
            else:
                set_for_one_category(
                    data, category_id, user_type, u"资讯", "news", news_categories)
                data[category_id]["effective_reading"] = ner[news_id]

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
                set_for_one_category(
                    data, k, user_type, u"资讯", "news", news_categories)
                data[k]["comments_count"] = int(v[1])
    # 获取收藏数
    sql = "select n.category_id, count(*) from user_like_news as uln join news as n on(uln.news_id = n.id) where uln.user_id in (%s) and uln.created_at >= \"%s\" and uln.created_at < \"%s\" group by n.category_id"
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql), day1, day2)
        rt = mysql_tool.querydb(
                sql_use, logger, "select user_like_news for %d users" % len(user_to_sql))
        for v in rt:
            k = int(v[0])
            if k in data.keys():
                if "like_count" in data[k].keys():
                    data[k]["like_count"] += int(v[1])
                else:
                    data[k]["like_count"] = int(v[1])
            else:
                set_for_one_category(
                    data, k, user_type, u"资讯", "news", news_categories)
                data[k]["like_count"] = int(v[1])
    # 获取点赞数
    sql = "select n.category_id, count(*) from user_zan_news_comments as uznc join news_comments as nc on (uznc.comment_id = nc.id) join news as n on(nc.news_id = n.id) where uznc.user_id in (%s) and uznc.created_at >= \"%s\" and uznc.created_at < \"%s\" group by n.category_id"
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql), day1, day2)
        rt = mysql_tool.querydb(
                sql_use, logger, "select user_zan_news_comments for %d users" % len(user_to_sql))
        for v in rt:
            k = int(v[0])
            if k in data.keys():
                if "zan_count" in data[k].keys():
                    data[k]["zan_count"] += int(v[1])
                else:
                    data[k]["zan_count"] = int(v[1])
            else:
                set_for_one_category(
                    data, k, user_type, u"资讯", "news", news_categories)
                data[k]["zan_count"] = int(v[1])

    return data


def set_for_one_category(data={}, category_id=1, user_type="new",
        channel_name="", channel="news", categories={}):
    data[category_id] = {}
    data[category_id]["user_type"] = user_type
    data[category_id]['channel_name'] = channel_name
    data[category_id]['channel'] = channel
    data[category_id]['category_id'] = category_id
    if category_id in categories.keys():
        data[category_id]['category_name'] = categories[category_id]


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
    ver = {}
    sob = mysql_tool.sql_obj()
    sql = "select video_id, count(*) from video_effective_readings where user_id in (%s) and created_at >= \"%s\" and created_at < \"%s\" and effective != 0 group by video_id"
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql), day1, day2)
        rt = sob.querydb(
            sql_use, logger, "select video_effective_readings for %d users" % len(user_to_sql))
        for v in rt:
            video_id = int(v[0])
            if video_id in ver.keys():
                ver[video_id] += int(v[1])
            else:
                ver[video_id] = int(v[1])
    sob.closedb()
    video_id_arr = [str(video_id) for video_id in ver.keys()]
    # 获取普通类目有效阅读
    sql = "select id, category_id from videos where id in (%s)"
    for i in range(0, len(video_id_arr), 1000):
        id_arr = video_id_arr[i:i+1000]
        sql_use = sql % (",".join(id_arr))
        rt = mysql_tool.querydb(
            sql_use, logger, "select id, category_id from videos for %d video" % len(id_arr))
        for v in rt:
            video_id = int(v[0])
            category_id = int(v[1])
            if category_id in data.keys():
                data[category_id]["effective_reading"] += ver[video_id]
            else:
                set_for_one_category(
                    data, category_id, user_type, u"视频", "video", video_categories)
                data[category_id]["effective_reading"] = ver[video_id]

    return data


def process(nday=1):
    new_user_arr = get_new_user_arr(nday)
    mysql_tool.connectdb(host="47.96.238.205", database="taozuiredian-news")
    mysql_tool.querydb("SET NAMES utf8mb4")
    data_news = get_edit_news_info(new_user_arr, "new", nday)
    data_video = get_edit_video_info(new_user_arr, "new", nday)
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
