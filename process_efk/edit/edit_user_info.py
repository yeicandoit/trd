# coding=utf-8
import requests
import json
import copy
import logging.config
import edit_info
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


def get_effective_reading(sql="", user_arr=[], message=""):
    er = {}
    sob = mysql_tool.sql_obj()
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql))
        rt = sob.querydb(sql_use, logger, message)
        for v in rt:
            reading_id = int(v[0])
            if reading_id in er.keys():
                er[reading_id] += int(v[1])
            else:
                er[reading_id] = int(v[1])
    sob.closedb()
    return er


def get_news_effective_readings(user_arr=[], nday=1):
    # 获取有效阅读
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    sql = "select news_id, count(*) from news_effective_readings where user_id in (%s) and created_at >= \"" + \
        day1 + "\" and created_at < \"" + day2 + \
        "\" and effective != 0 group by news_id"
    ner = get_effective_reading(
        sql, user_arr, "select from news_effective_readings")
    return ner


def get_edit_news_info(user_arr=[], user_type="new", ner={}, nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)

    # 获取资讯名称
    news_categories = {}
    sql = "select id, name from news_categories"
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        news_categories[int(v[0])] = v[1]

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
    sql = "select n.category_id, count(*) from news_comments as nc join news as n on (nc.news_id = n.id) where nc.user_id in (%s) and nc.created_at >= \"" + \
        day1 + "\" and nc.created_at < \"" + day2 + "\" group by n.category_id"
    set_common_edit_info(sql, user_arr, data, "select from news_comments",
                         "comments_count", user_type, "news", u"资讯", news_categories)
    # 获取收藏数
    sql = "select n.category_id, count(*) from user_like_news as uln join news as n on(uln.news_id = n.id) where uln.user_id in (%s) and uln.created_at >= \"" + \
        day1 + "\" and uln.created_at < \"" + day2 + "\" group by n.category_id"
    set_common_edit_info(sql, user_arr, data, "select from user_like_news",
                         "like_count", user_type, "news", u"资讯", news_categories)
    # 获取点赞数
    sql = "select n.category_id, count(*) from user_zan_news_comments as uznc join news_comments as nc on (uznc.comment_id = nc.id) join news as n on(nc.news_id = n.id) where uznc.user_id in (%s) and uznc.created_at >= \"" + \
        day1 + "\" and uznc.created_at < \"" + day2 + "\" group by n.category_id"
    set_common_edit_info(sql, user_arr, data, "select from user_zan_news_comments",
                         "zan_count", user_type, "news", u"资讯", news_categories)

    return data


def set_common_edit_info(sql="", user_arr=[], data={}, message="", fkey="",
                         user_type="", channel="", channel_name="", categories={}):
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql))
        rt = mysql_tool.querydb(sql_use, logger, message)
        for v in rt:
            k = int(v[0])
            if k in data.keys():
                if fkey in data[k].keys():
                    data[k][fkey] += int(v[1])
                else:
                    data[k][fkey] = int(v[1])
            else:
                set_for_one_category(data, k, user_type,
                                     channel_name, channel, categories)
                data[k][fkey] = int(v[1])


def set_for_one_category(data={}, category_id=1, user_type="new",
                         channel_name="", channel="news", categories={}):
    data[category_id] = {}
    data[category_id]["user_type"] = user_type
    data[category_id]['channel_name'] = channel_name
    data[category_id]['channel'] = channel
    data[category_id]['category_id'] = category_id
    if category_id in categories.keys():
        data[category_id]['category_name'] = categories[category_id]


def get_hot_news_info(user_arr=[], ner={}, data={}, nday=1):
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    data[edit_info.HOT_NEWS_CATEGORY_ID] = {}
    data[edit_info.HOT_NEWS_CATEGORY_ID]['user_type'] = "new"
    data[edit_info.HOT_NEWS_CATEGORY_ID]['channel_name'] = "资讯"
    data[edit_info.HOT_NEWS_CATEGORY_ID]['channel'] = "news"
    data[edit_info.HOT_NEWS_CATEGORY_ID]['category_id'] = edit_info.HOT_NEWS_CATEGORY_ID
    data[edit_info.HOT_NEWS_CATEGORY_ID]['category_name'] = edit_info.HOT_SHOW

    # 获取热点资讯有效阅读
    news_id_arr = [str(news_id) for news_id in ner.keys()]
    sql = "select id from news where id in (%s) and hot_at < \"" + day2 + "\""
    for i in range(0, len(news_id_arr), 1000):
        id_arr = news_id_arr[i:i+1000]
        sql_use = sql % (",".join(id_arr))
        rt = mysql_tool.querydb(
            sql_use, logger, "select id from news for %d news" % len(id_arr))
        for v in rt:
            news_id = int(v[0])
            if 'effective_reading' in data[edit_info.HOT_NEWS_CATEGORY_ID].keys():
                data[edit_info.HOT_NEWS_CATEGORY_ID]['effective_reading'] += ner[news_id]
            else:
                data[edit_info.HOT_NEWS_CATEGORY_ID]['effective_reading'] = ner[news_id]

    # 获取热点资讯评论数
    sql = "select count(*) from news_comments as nc join news as n on(nc.news_id = n.id) where n.id in (select id from news where hot_at < \"" + \
        day2 + "\") and nc.created_at >= \"" + day1 + \
        "\" and nc.created_at < \"" + day2 + "\" and nc.user_id in (%s)"
    set_common_hot_info(
        sql, user_arr, data[edit_info.HOT_NEWS_CATEGORY_ID], 'comments_count', "select from news_comments")

    # 获取热点资讯收藏数
    sql = "select count(*) from user_like_news as uln join news as n on(uln.news_id = n.id) where n.id in (select id from news where hot_at < \"" + \
        day2 + "\") and uln.created_at >= \"" + day1 + \
        "\" and uln.created_at < \"" + day2 + "\" and uln.user_id in (%s)"
    set_common_hot_info(
        sql, user_arr, data[edit_info.HOT_NEWS_CATEGORY_ID], 'like_count', "select from user_like_news")

    # 获取热点资讯点赞数
    sql = "select count(*) from user_zan_news_comments as uznc join news_comments as nc on (uznc.comment_id = nc.id) join news as n on(nc.news_id = n.id) where n.id in (select id from news where hot_at < \"" + \
        day2 + "\") and uznc.created_at >= \"" + day1 + \
        "\" and uznc.created_at < \"" + day2 + "\" and uznc.user_id in (%s)"
    set_common_hot_info(sql, user_arr, data[edit_info.HOT_NEWS_CATEGORY_ID],
                        'zan_count', "select from user_zan_news_comments")

    return data


def set_common_hot_info(sql, user_arr=[], data_hot={}, fk="", message=""):
    for i in range(0, len(user_arr), 1000):
        user_to_sql = user_arr[i:i+1000]
        sql_use = sql % (",".join(user_to_sql))
        rt = mysql_tool.querydb(sql_use, logger, message)
        for v in rt:
            if fk in data_hot.keys():
                data_hot[fk] += int(v[0])
            else:
                data_hot[fk] = int(v[0])


def get_hot_list_info(user_arr=[], ner={}, data={}, nday=1):
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    for table, category_id, category_name in [
            ("news_hot_lists", edit_info.NEWS_HOT_LISTS,
             edit_info.NEWS_HOT_LISTS_SHOW),
            ("news_hot_seven_day_lists", edit_info.NEWS_HOT_SEVEN,
             edit_info.NEWS_HOT_SEVEN_SHOW),
            ("news_hot_total_lists", edit_info.NEWS_HOT_TOTAL, edit_info.NEWS_HOT_TOTAL_SHOW)]:
        data[category_id] = {}
        data[category_id]['user_type'] = "new"
        data[category_id]['channel_name'] = u"资讯"
        data[category_id]['channel'] = "news"
        data[category_id]['category_id'] = category_id
        data[category_id]['category_name'] = category_name

        # 获取有效阅读
        news_id_arr = [str(news_id) for news_id in ner.keys()]
        sql = "select news_id from " + table + " where news_id in (%s)"
        for i in range(0, len(news_id_arr), 1000):
            id_arr = news_id_arr[i:i+1000]
            sql_use = sql % (",".join(id_arr))
            rt = mysql_tool.querydb(
                sql_use, logger, "select news_id from %s for %d news" % (table, len(id_arr)))
            for v in rt:
                news_id = int(v[0])
                if 'effective_reading' in data[category_id].keys():
                    data[category_id]['effective_reading'] += ner[news_id]
                else:
                    data[category_id]['effective_reading'] = ner[news_id]

        # 获取评论数
        sql = "select count(*) from news_comments as nc join news as n on(nc.news_id = n.id) where n.id in (select news_id from " + \
            table + ") and nc.created_at >= \"" + day1 + \
            "\" and nc.created_at < \"" + day2 + "\" and nc.user_id in (%s)"
        set_common_hot_info(
            sql, user_arr, data[category_id], 'comments_count', "select news_comments")

        # 获取收藏数
        sql = "select count(*) from user_like_news as uln join news as n on(uln.news_id = n.id) where n.id in (select news_id from " + \
            table + ") and uln.created_at >= \"" + day1 + \
            "\" and uln.created_at < \"" + day2 + "\" and uln.user_id in (%s)"
        set_common_hot_info(
            sql, user_arr, data[category_id], 'like_count', "select user_like_newsews_comments")

        # 获取点赞数
        sql = "select count(*) from user_zan_news_comments as uznc join news_comments as nc on (uznc.comment_id = nc.id) join news as n on(nc.news_id = n.id) where n.id in (select news_id from " + \
            table + ") and uznc.created_at >= \"" + day1 + \
            "\" and uznc.created_at < \"" + \
            day2 + "\" and uznc.user_id in (%s)"
        set_common_hot_info(
            sql, user_arr, data[category_id], 'zan_count', "select user_zan_news_comments")

    return data


def get_video_effective_readings(user_arr, nday=1):
    # 获取有效阅读
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    sql = "select video_id, count(*) from video_effective_readings where user_id in (%s) and created_at >= \"" + \
        day1 + "\" and created_at < \"" + day2 + \
        "\" and effective != 0 group by video_id"
    ver = get_effective_reading(
        sql, user_arr, "select from video_effective_readings")

    return ver


def get_edit_video_info(user_arr=[], user_type="new", ver={}, nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)

    # 获取视频名称
    video_categories = {}
    sql = "select id, name from video_categories"
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        video_categories[int(v[0])] = v[1]

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

    # 获取热点视频有效阅读
    sql = "select id from videos where id in (%s) and hot_at < \"" + \
        day2 + "\""
    for i in range(0, len(video_id_arr), 1000):
        id_arr = video_id_arr[i:i+1000]
        sql_use = sql % (",".join(id_arr))
        rt = mysql_tool.querydb(
            sql_use, logger, "select id from videos for %d video" % len(id_arr))
        for v in rt:
            video_id = int(v[0])
            if edit_info.HOT_VIDEO_CATEGORY_ID in data.keys():
                data[edit_info.HOT_VIDEO_CATEGORY_ID]['effective_reading'] += ver[video_id]
            else:
                data[edit_info.HOT_VIDEO_CATEGORY_ID] = {}
                data[edit_info.HOT_VIDEO_CATEGORY_ID]['effective_reading'] = ver[video_id]
                data[edit_info.HOT_VIDEO_CATEGORY_ID]['user_type'] = "new"
                data[edit_info.HOT_VIDEO_CATEGORY_ID]['channel_name'] = u"视频"
                data[edit_info.HOT_VIDEO_CATEGORY_ID]['channel'] = "video"
                data[edit_info.HOT_VIDEO_CATEGORY_ID]['category_id'] = edit_info.HOT_VIDEO_CATEGORY_ID
                data[edit_info.HOT_VIDEO_CATEGORY_ID]['category_name'] = edit_info.HOT_SHOW

    return data


def set_a_key(h1={}, h2={}, h3={}, k=1):
    if k in h1.keys() and k in h2.keys():
        h3[k] = h2[k] - h1[k]


def process(nday=1):
    new_user_arr = get_new_user_arr(nday)
    mysql_tool.connectdb(host="47.96.238.205", database="taozuiredian-news")
    mysql_tool.querydb("SET NAMES utf8mb4")
    ner = get_news_effective_readings(new_user_arr, nday)
    data_news = get_edit_news_info(new_user_arr, "new", ner, nday)
    data_news = get_hot_news_info(new_user_arr, ner, data_news, nday)
    data_news = get_hot_list_info(new_user_arr, ner, data_news, nday)
    ver = get_video_effective_readings(new_user_arr, nday)
    data_video = get_edit_video_info(new_user_arr, "new", ver, nday)
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
            url_edit = edit_info.URL_ELASTICSEARCH_EDIT_INFO + \
                "/" + _id + "_" + v['channel'] + "_" + str(k)
            r = requests.get(url_edit, headers=JSON_HEADER, timeout=(30, 120))
            if 200 != r.status_code:
                logger.error("request edit_user_info failed, status_code:%d, reason:%s",
                             r.status_code, json.dumps(r.json()))
            else:
                r_json = r.json()
                if "_source" in r_json.keys():
                    info = r_json["_source"]
                    v_ = copy.deepcopy(v)
                    v_["user_type"] = "not_new"
                    set_a_key(v, info, v_, "effective_reading")
                    set_a_key(v, info, v_, "comments_count")
                    set_a_key(v, info, v_, "like_count")
                    set_a_key(v, info, v_, "zan_count")
                    url_ = URL_ELASTICSEARCH_EDIT_USER_INFO + \
                        "/" + _id + "_not_new_" + v['channel'] + "_" + str(k)
                    r = requests.post(url_, headers=JSON_HEADER,
                                      data=json.dumps(v_), timeout=(30, 120))
                    if 200 != r.status_code and 201 != r.status_code:
                        logger.error("request edit index failed, status_code:%d, reason:%s",
                                     r.status_code, json.dumps(r.json()))


if __name__ == '__main__':
    process()
