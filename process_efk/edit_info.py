# coding=utf-8
import requests
import json
import logging.config
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_EDIT_INFO = "http://localhost:9200/edit_info/doc"
JSON_HEADER = {"Content-Type": "application/json"}
HOT_NEWS_CATEGORY_ID = 99990
HOT_VIDEO_CATEGORY_ID = 99991
HOT_SHOW = u"热点"
NEWS_HOT_LISTS = 99992
NEWS_HOT_LISTS_SHOW = "蹿红榜"
NEWS_HOT_SEVEN = 99993
NEWS_HOT_SEVEN_SHOW = "七天榜"
NEWS_HOT_TOTAL = 99994
NEWS_HOT_TOTAL_SHOW = "总榜"

TOTAL_ID = 10000
TOTAL_SHOW = "汇总"

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/edit_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "channel":{"type":"keyword"}, "channel_name":{"type":"keyword"}, "category_id":{"type":"long"}, "category_name":{"type":"keyword"}, "pv":{"type":"long"},"pv_total":{"type":"long"}, "effective_reading":{"type":"long"}, "like_count":{"type":"long"}, "like_count_total":{"type":"long"}, "comments_count":{"type":"long"}, "comments_count_total":{"type":"long"}, "new_count":{"type":"long"}, "zan_count":{"type":"long"}, "new_choosed_count":{"type":"long"}, "new_published_count":{"type":"long"}, "yd_choosed_count":{"type":"long"}, "old_published_percentage":{"type":"long"}, "dau_count":{"type":"long"}, "share_count":{"type":"long"}, "pv_dau":{"type":"float"}, "pv_published":{"type":"float"}, "reading_pv":{"type":"float"}, "comments_pv":{"type":"float"}, "zan_pv":{"type":"float"}, "like_pv":{"type":"float"}, "share_pv":{"type":"float"}}}'


def get_active_user_num(nday=1):
    start = time_tool.get_weehours_of_someday(-nday)
    query = {
        "size": 0,
        "aggs": {
            "user_num": {
                "cardinality": {
                    "field": "user_id.keyword",
                 }
            }
        },
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

    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        return r_json['aggregations']['user_num']['value']
    else:
        logger.error("request applog index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return 0


def get_share_num(nday=1):
    start = time_tool.get_weehours_of_someday(-nday)
    query = {
        "size": 0,
        "_source": {
            "excludes": []
        },
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
                ],
                "must": [
                    {
                        "match_phrase": {
                            "key.keyword": {
                                "query": "app_news_share_button_click"
                            }
                        }
                    }
                ]
            }
        }
    }

    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        return r_json['hits']['total']
    else:
        logger.error("request applog index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return 0

def get_edit_news_info(nday=1):
    data = {}
    day0 = time_tool.get_someday_str(-nday-1)
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    tommorow = time_tool.get_someday_str(1)

    # 获取资讯名称
    news_categories = {}
    sql = "select id, name from news_categories"
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        news_categories[int(v[0])] = v[1]

    # 获取当日发布资讯
    news_new_published_count = {}
    sql = "select category_id, count(*) from news where published_at >= \"%s\" and published_at < \"%s\" group by category_id" % (day1, day2)
    handle_sql_hash_total(news_new_published_count, sql, logger, TOTAL_ID)

    # 获取今日发布的旧资讯占比数
    news_old_published_percentage = {}
    sql = "select category_id, count(*) from news where published_at >= \"%s\" and published_at < \"%s\" and created_at < \"%s\" group by category_id" % (day1, day2, day1)
    rt = mysql_tool.querydb(sql, logger, sql)
    total = 0
    for v in rt:
        k = int(v[0])
        total += int(v[1])
        if k in news_new_published_count.keys() and news_new_published_count[k] != 0:
            news_old_published_percentage[k] = round(
                float(v[1])/news_new_published_count[k], 2)
    if TOTAL_ID in news_new_published_count.keys() and news_new_published_count[TOTAL_ID] != 0:
        news_old_published_percentage[TOTAL_ID] = round(
            float(total)/news_new_published_count[TOTAL_ID], 2)

    # 获取当日挑选资讯
    news_new_chooseed_count = {}
    sql = "select category_id, count(*) from news_raw_auto_releases where created_at >= \"%s\" and created_at < \"%s\" group by category_id" % (day1, day2)
    handle_sql_hash_total(news_new_chooseed_count, sql, logger, TOTAL_ID)

    # 获取昨天挑选资讯
    news_yd_choosed_count = {}
    sql = "select category_id, count(*) from news_raw_auto_releases where created_at >= \"%s\" and created_at < \"%s\" group by category_id" % (day0, day1)
    handle_sql_hash_total(news_yd_choosed_count, sql, logger, TOTAL_ID)

    # 获取评论数
    comments_count_hash = {}
    sql = "select n.category_id, count(*) from news_comments as nc join news as n on(nc.news_id = n.id) where nc.created_at >= \"%s\" and nc.created_at < \"%s\" group by n.category_id" % (day1, day2)
    handle_sql_hash_total(comments_count_hash, sql, logger, TOTAL_ID)

    # 获取收藏数
    like_count_hash = {}
    sql = "select n.category_id, count(*) from user_like_news as uln join news as n on(uln.news_id = n.id) where uln.created_at >= \"%s\" and uln.created_at < \"%s\" group by n.category_id" % (day1, day2)
    handle_sql_hash_total(like_count_hash, sql, logger, TOTAL_ID)

    # 获取点赞数, 没有用 news_comments 获取点赞，是因为用户可以在当天评论昨天或更早的文章，这样用news_comments表就无法统计了
    zan_count_hash = {}
    sql = "select n.category_id, count(*) from user_zan_news_comments as uznc join news_comments as nc on (uznc.comment_id = nc.id) join news as n on(nc.news_id = n.id) where uznc.created_at >= \"%s\" and uznc.created_at < \"%s\" group by n.category_id" % (day1, day2)
    handle_sql_hash_total(zan_count_hash, sql, logger, TOTAL_ID)

    # 获取pv
    sql = "select category_id, sum(real_pv) from news group by category_id"
    rt = mysql_tool.querydb(sql, logger, sql)
    total = 0
    for v in rt:
        k = int(v[0])
        total += int(v[1])
        data[k] = {}
        data[k]['channel_name'] = u"资讯"
        data[k]['channel'] = "news"
        data[k]['category_id'] = k
        data[k]['pv_total'] = int(v[1])
        set_hash(news_categories, data[k], k, 'category_name')
        set_hash(news_new_published_count, data[k], k, 'new_published_count')
        set_hash(news_new_chooseed_count, data[k], k, 'new_choosed_count')
        set_hash(news_yd_choosed_count, data[k], k, 'yd_choosed_count')
        set_hash(news_old_published_percentage,
                 data[k], k, 'old_published_percentage')
        set_hash(comments_count_hash, data[k], k, 'comments_count')
        set_hash(like_count_hash, data[k], k, 'like_count')
        set_hash(zan_count_hash, data[k], k, 'zan_count')
    data[TOTAL_ID] = {}
    data[TOTAL_ID]['channel_name'] = u"资讯"
    data[TOTAL_ID]['channel'] = "news"
    data[TOTAL_ID]['category_id'] = TOTAL_ID
    data[TOTAL_ID]['pv_total'] = total
    data[TOTAL_ID]['category_name'] = TOTAL_SHOW
    set_hash(news_new_published_count, data[TOTAL_ID], TOTAL_ID, 'new_published_count')
    set_hash(news_new_chooseed_count, data[TOTAL_ID], TOTAL_ID, 'new_choosed_count')
    set_hash(news_yd_choosed_count, data[TOTAL_ID], TOTAL_ID, 'yd_choosed_count')
    set_hash(news_old_published_percentage,
             data[TOTAL_ID], TOTAL_ID, 'old_published_percentage')
    set_hash(comments_count_hash, data[TOTAL_ID], TOTAL_ID, 'comments_count')
    set_hash(like_count_hash, data[TOTAL_ID], TOTAL_ID, 'like_count')
    set_hash(zan_count_hash, data[TOTAL_ID], TOTAL_ID, 'zan_count')
    data[TOTAL_ID]['dau_count'] = get_active_user_num(nday)
    data[TOTAL_ID]['share_count'] = get_share_num(nday)

    return data


def handle_sql_hash(mhash={}, sql="", lg=logger):
    rt = mysql_tool.querydb(sql, lg, sql)
    for v in rt:
        mhash[int(v[0])] = int(v[1])


def handle_sql_hash_total(mhash={}, sql="", lg=logger, hk=1):
    rt = mysql_tool.querydb(sql, lg, sql)
    total = 0
    for v in rt:
        total += int(v[1])
        mhash[int(v[0])] = int(v[1])
    mhash[hk] = total


def set_hash(shash={}, dhash={}, key1=1, key2=1):
    if key1 in shash.keys():
        dhash[key2] = shash[key1]


def get_hot_news_info(nday=1, data={}):
    day0 = time_tool.get_someday_str(-nday-1)
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    tommorow = time_tool.get_someday_str(1)

    # 获取当天发布热点资讯
    hot_new_published_count = 0
    data[HOT_NEWS_CATEGORY_ID] = {}
    sql = "select count(*) from news where hot_at >= \"%s\" and hot_at < \"%s\" and published_at < \"%s\"" % (day1, day2, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_NEWS_CATEGORY_ID]['new_published_count'] = int(v[0])
        hot_new_published_count = int(v[0])

    # 获取今日发布的热点旧资讯占比数
    sql = "select count(*) from news where hot_at >= \"%s\" and hot_at < \"%s\" and published_at < \"%s\" and created_at < \"%s\"" % (day1, day2, day2, day1)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        if hot_new_published_count > 0:
            data[HOT_NEWS_CATEGORY_ID]['old_published_percentage'] = round(
                float(v[0])/hot_new_published_count, 2)

    # 获取当天挑选热点资讯
    sql = "select count(*) from news_raw_auto_releases where created_at >= \"%s\" and created_at < \"%s\" and is_hot !=0" % (day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_NEWS_CATEGORY_ID]['new_choosed_count'] = int(v[0])

    # 获取昨天挑选热点资讯
    sql = "select count(*) from news_raw_auto_releases where created_at >= \"%s\" and created_at < \"%s\" and is_hot !=0" % (day0, day1)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_NEWS_CATEGORY_ID]['yd_choosed_count'] = int(v[0])

    # 获取热点资讯评论数
    sql = "select count(*) from news_comments as nc join news as n on(nc.news_id = n.id) where n.id in (select id from news where hot_at < \"%s\") and nc.created_at >= \"%s\" and nc.created_at < \"%s\"" % (day2, day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_NEWS_CATEGORY_ID]['comments_count'] = int(v[0])

    # 获取热点资讯收藏数
    sql = "select count(*) from user_like_news as uln join news as n on(uln.news_id = n.id) where n.id in (select id from news where hot_at < \"%s\") and uln.created_at >= \"%s\" and uln.created_at < \"%s\"" % (day2, day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_NEWS_CATEGORY_ID]['like_count'] = int(v[0])

    # 获取热点资讯点赞数
    sql = "select count(*) from user_zan_news_comments as uznc join news_comments as nc on (uznc.comment_id = nc.id) join news as n on(nc.news_id = n.id) where n.id in (select id from news where hot_at < \"%s\") and uznc.created_at >= \"%s\" and uznc.created_at < \"%s\"" % (day2, day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_NEWS_CATEGORY_ID]['zan_count'] = int(v[0])

    # 获取热点资讯的pv.
    sql = "select sum(real_pv) from news where hot_at < \"%s\"" % tommorow
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_NEWS_CATEGORY_ID]['channel_name'] = u"资讯"
        data[HOT_NEWS_CATEGORY_ID]['channel'] = "news"
        data[HOT_NEWS_CATEGORY_ID]['category_id'] = HOT_NEWS_CATEGORY_ID
        data[HOT_NEWS_CATEGORY_ID]['pv_total'] = int(v[0])
        data[HOT_NEWS_CATEGORY_ID]['category_name'] = HOT_SHOW

    return data


# 获取榜单数据
def get_hot_list_info(nday=1, data={}):
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)

    for table, category_id, category_name in [("news_hot_lists", NEWS_HOT_LISTS, NEWS_HOT_LISTS_SHOW),
                                              ("news_hot_seven_day_lists",
                                               NEWS_HOT_SEVEN, NEWS_HOT_SEVEN_SHOW),
                                              ("news_hot_total_lists", NEWS_HOT_TOTAL, NEWS_HOT_TOTAL_SHOW)]:
        data[category_id] = {}
        # 获取新增
        sql = "select count(*) from %s where created_at >= \"%s\" and created_at < \"%s\"" % (
            table, day1, day2)
        rt = mysql_tool.querydb(sql, logger, sql)
        for v in rt:
            data[category_id]['new_published_count'] = int(v[0])

        # 获取评论数
        sql = "select count(*) from news_comments as nc join news as n on(nc.news_id = n.id) where n.id in (select news_id from %s) and nc.created_at >= \"%s\" and nc.created_at < \"%s\"" % (table, day1, day2)
        rt = mysql_tool.querydb(sql, logger, sql)
        for v in rt:
            data[category_id]['comments_count'] = int(v[0])

        # 获取收藏数
        sql = "select count(*) from user_like_news as uln join news as n on(uln.news_id = n.id) where n.id in (select news_id from %s) and uln.created_at >= \"%s\" and uln.created_at < \"%s\"" % (table, day1, day2)
        rt = mysql_tool.querydb(sql, logger, sql)
        for v in rt:
            data[category_id]['like_count'] = int(v[0])

        # 获取点赞数
        sql = "select count(*) from user_zan_news_comments as uznc join news_comments as nc on (uznc.comment_id = nc.id) join news as n on(nc.news_id = n.id) where n.id in (select news_id from %s) and uznc.created_at >= \"%s\" and uznc.created_at < \"%s\"" % (table, day1, day2)
        rt = mysql_tool.querydb(sql, logger, sql)
        for v in rt:
            data[category_id]['zan_count'] = int(v[0])

        # 获取pv.
        sql = "select sum(real_pv) from news where id in (select news_id from %s)" % table
        rt = mysql_tool.querydb(sql, logger, sql)
        for v in rt:
            data[category_id]['channel_name'] = u"资讯"
            data[category_id]['channel'] = "news"
            data[category_id]['category_id'] = category_id
            data[category_id]['pv_total'] = int(v[0])
            data[category_id]['category_name'] = category_name

    return data


def get_edit_video_info(nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    tommorow = time_tool.get_someday_str(1)

    # 获取视频名称
    video_categories = {}
    sql = "select id, name from video_categories"
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        video_categories[int(v[0])] = v[1]

    # 获取新增视频
    video_new_count = {}
    sql = "select category_id, count(*) from videos where published_at >= \"%s\" and published_at < \"%s\" group by category_id" % (day1, day2)
    handle_sql_hash_total(video_new_count, sql, logger, TOTAL_ID)

    # 获取视频pv
    sql = "select category_id, sum(real_pv) from videos group by category_id"
    rt = mysql_tool.querydb(sql, logger, sql)
    total = 0
    for v in rt:
        k = int(v[0])
        total += int(v[1])
        data[k] = {}
        data[k]['channel_name'] = u"视频"
        data[k]['channel'] = "video"
        data[k]['category_id'] = k
        data[k]['pv_total'] = int(v[1])
        set_hash(video_categories, data[k], k, 'category_name')
        set_hash(video_new_count, data[k], k, 'new_published_count')
    data[TOTAL_ID] = {}
    data[TOTAL_ID]['channel_name'] = u"视频"
    data[TOTAL_ID]['channel'] = "video"
    data[TOTAL_ID]['category_name'] = TOTAL_SHOW
    data[TOTAL_ID]['category_id'] = TOTAL_ID
    data[TOTAL_ID]['pv_total'] = total
    set_hash(video_new_count, data[TOTAL_ID], TOTAL_ID, 'new_published_count')

    # 获取热点视频
    data[HOT_VIDEO_CATEGORY_ID] = {}
    sql = "select count(*) from videos where hot_at >= \"%s\" and hot_at < \"%s\" and published_at < \"%s\"" % (day1, day2, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_VIDEO_CATEGORY_ID]['new_published_count'] = int(v[0])

    # 获取热点视频的pv
    sql = "select sum(real_pv) from videos where hot_at < \"%s\"" % tommorow
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_VIDEO_CATEGORY_ID]['channel_name'] = u"视频"
        data[HOT_VIDEO_CATEGORY_ID]['channel'] = "video"
        data[HOT_VIDEO_CATEGORY_ID]['category_id'] = HOT_VIDEO_CATEGORY_ID
        data[HOT_VIDEO_CATEGORY_ID]['pv_total'] = int(v[0])
        data[HOT_VIDEO_CATEGORY_ID]['category_name'] = HOT_SHOW

    return data


def set_pv_(mhash={}, k1=1, k2=1, k3=1):
    if k1 in mhash.keys() and k2 in mhash.keys() and mhash[k2] > 0:
        mhash[k3] = float(mhash[k1]) / mhash[k2]


def get_news_effective_reading(nday=1, data_news={}):
    # 获取资讯有效阅读
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    ner = {}
    sql = "select news_id, count(*) from news_effective_readings where created_at >= \"%s\" and created_at < \"%s\" and effective != 0 group by news_id" % (day1, day2)
    sob = mysql_tool.sql_obj()
    rt = sob.querydb(sql, logger, sql)
    total = 0 
    for v in rt:
        ner[int(v[0])] = int(v[1])
        total += int(v[1])
    sob.closedb()
    data_news[TOTAL_ID]['effective_reading'] =  total
    news_id_arr = [str(news_id) for news_id in ner.keys()]
    # 获取普通类目有效阅读
    sql = "select id, category_id from news where id in (%s)"
    for i in range(0, len(news_id_arr), 1000):
        id_arr = news_id_arr[i:i+1000]
        sql_use = sql % (",".join(id_arr))
        rt = mysql_tool.querydb(sql_use, logger, "select id, category_id from news for %d news" % len(id_arr))
        for v in rt:
            news_id = int(v[0])
            category_id = int(v[1])
            if category_id in data_news.keys():
                if 'effective_reading' in data_news[category_id].keys():
                    data_news[category_id]['effective_reading'] += ner[news_id]
                else:
                    data_news[category_id]['effective_reading'] = ner[news_id]
    # 获取热点有效阅读
    if HOT_NEWS_CATEGORY_ID in data_news.keys():
        sql = "select id from news where id in (%s) and hot_at < \"" + day2 + "\""
        for i in range(0, len(news_id_arr), 1000):
            id_arr = news_id_arr[i:i+1000]
            sql_use = sql % (",".join(id_arr))
            rt = mysql_tool.querydb(sql_use, logger, "select id from news for %d news" % len(id_arr))
            for v in rt:
                news_id = int(v[0])
                if 'effective_reading' in data_news[HOT_NEWS_CATEGORY_ID].keys():
                    data_news[HOT_NEWS_CATEGORY_ID]['effective_reading'] += ner[news_id]
                else:
                    data_news[HOT_NEWS_CATEGORY_ID]['effective_reading'] = ner[news_id]
    # 获取榜单有效阅读
    for table, category_id in [("news_hot_lists", NEWS_HOT_LISTS), 
            ("news_hot_seven_day_lists", NEWS_HOT_SEVEN), 
            ("news_hot_total_lists", NEWS_HOT_TOTAL)]:
        if category_id in data_news.keys():
            sql = "select news_id from %s where news_id in (%s)"
            for i in range(0, len(news_id_arr), 1000):
                id_arr = news_id_arr[i:i+1000]
                sql_use = sql % (table, ",".join(id_arr))
                rt = mysql_tool.querydb(sql_use, logger, "select news_id for %d %s news" % (len(id_arr), table))
                for v in rt:
                    news_id = int(v[0])
                    if 'effective_reading' in data_news[category_id].keys():
                        data_news[category_id]['effective_reading'] += ner[news_id]
                    else:
                        data_news[category_id]['effective_reading'] = ner[news_id]

    return data_news

def get_video_effective_reading(nday=1, data_video={}):
    # 获取视频有效阅读
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    ver = {}
    sql = "select video_id, count(*) from video_effective_readings where created_at >= \"%s\" and created_at < \"%s\" and effective != 0 group by video_id" % (day1, day2)
    sob = mysql_tool.sql_obj()
    rt = sob.querydb(sql, logger, sql)
    total = 0 
    for v in rt:
        ver[int(v[0])] = int(v[1])
        total += int(v[1])
    sob.closedb()
    data_video[TOTAL_ID]['effective_reading'] =  total
    video_id_arr = [str(video_id) for video_id in ver.keys()]
    # 获取普通类目有效阅读
    sql = "select id, category_id from videos where id in (%s)"
    for i in range(0, len(video_id_arr), 1000):
        id_arr = video_id_arr[i:i+1000]
        sql_use = sql % (",".join(id_arr))
        rt = mysql_tool.querydb(sql_use, logger, "select id, category_id from videos for %d videos" % len(id_arr))
        for v in rt:
            video_id = int(v[0])
            category_id = int(v[1])
            if category_id in data_video.keys():
                if 'effective_reading' in data_video[category_id].keys():
                    data_video[category_id]['effective_reading'] += ver[video_id]
                else:
                    data_video[category_id]['effective_reading'] = ver[video_id]
    # 获取热点有效阅读
    if HOT_VIDEO_CATEGORY_ID in data_video.keys():
        sql = "select id from videos where id in (%s) and hot_at < \"" + day2 + "\""
        for i in range(0, len(video_id_arr), 1000):
            id_arr = video_id_arr[i:i+1000]
            sql_use = sql % (",".join(id_arr))
            rt = mysql_tool.querydb(sql_use, logger, "select id from hot videos for %d videos" % len(id_arr))
            for v in rt:
                video_id = int(v[0])
                if 'effective_reading' in data_video[HOT_VIDEO_CATEGORY_ID].keys():
                    data_video[HOT_VIDEO_CATEGORY_ID]['effective_reading'] += ver[video_id]
                else:
                    data_video[HOT_VIDEO_CATEGORY_ID]['effective_reading'] = ver[video_id]
    return data_video


def process(nday=1):
    mysql_tool.connectdb(host="47.96.238.205", database="taozuiredian-news")
    mysql_tool.querydb("SET NAMES utf8mb4")
    data_news = get_edit_news_info(nday)
    data_news = get_hot_news_info(nday, data_news)
    data_news = get_hot_list_info(nday, data_news)
    data_news = get_news_effective_reading(nday, data_news)
    data_video = get_edit_video_info(nday)
    data_video = get_video_effective_reading(nday, data_video)
    mysql_tool.closedb()

    for data in [data_news, data_video]:
        for k, v in data.items():
            # 获取前一天
            yid = time_tool.get_someday_str(-nday-1)
            url = URL_ELASTICSEARCH_EDIT_INFO + \
                "/" + yid + "_" + v["channel"] + "_" + str(k)
            r = requests.get(url, headers=JSON_HEADER, timeout=(30, 120))
            if 200 != r.status_code:
                logger.error("request edit_info index failed, status_code:%d, reason:%s, k:%s",
                             r.status_code, r.reason, k)
            else:
                r_json = r.json()
                if "_source" in r_json.keys():
                    yd = r_json["_source"]
                    if "pv_total" in yd.keys():
                        v["pv"] = v["pv_total"] - yd["pv_total"]
                        set_pv_(v, "pv", "dau_count", "pv_dau")
                        set_pv_(v, "pv", "new_published_count", "pv_published")
                        set_pv_(v, "effective_reading", "pv", "reading_pv")
                        set_pv_(v, "comments_count", "pv", "comments_pv")
                        set_pv_(v, "zan_count", "pv", "zan_pv")
                        set_pv_(v, "like_count", "pv", "like_pv")
                        set_pv_(v, "share_count", "pv", "share_pv")
            v['@timestamp'] = time_tool.get_someday_es_format(-nday)
            _id = time_tool.get_someday_str(-nday)
            url = URL_ELASTICSEARCH_EDIT_INFO + \
                "/" + _id + "_" + v['channel'] + "_" + str(k)
            r = requests.post(url, headers=JSON_HEADER,
                              data=json.dumps(v), timeout=(30, 120))
            if 200 != r.status_code and 201 != r.status_code:
                logger.error("request edit index failed, status_code:%d, reason:%s, %s, %s",
                             r.status_code, json.dumps(r.json()), url, json.dumps(v))


if __name__ == '__main__':
    process()
