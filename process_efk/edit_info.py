# coding=utf-8
import requests
import json
import logging.config
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
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

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/edit_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "channel":{"type":"keyword"}, "channel_name":{"type":"keyword"}, "category_id":{"type":"long"}, "category_name":{"type":"keyword"}, "pv":{"type":"long"},"pv_total":{"type":"long"}, "effective_reading":{"type":"long"}, "like_count":{"type":"long"}, "like_count_total":{"type":"long"}, "comments_count":{"type":"long"}, "comments_count_total":{"type":"long"}, "new_count":{"type":"long"}, "zan_count":{"type":"long"}}}'


def get_edit_news_info(nday=1):
    data = {}
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    tommorow = time_tool.get_someday_str(1)

    # 获取资讯名称
    news_categories = {}
    sql = "select id, name from news_categories"
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        news_categories[int(v[0])] = v[1]

    # 获取新增资讯
    news_new_count = {}
    sql = "select category_id, count(*) from news where created_at >= \"%s\" and created_at < \"%s\" group by category_id" % (day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        news_new_count[int(v[0])] = int(v[1])

    # 获取有效阅读
    news_effective_reading = {}
    # sql = "select n.category_id, count(*) from news_effective_readings as ner join news as n on (ner.news_id = n.id) where ner.created_at >= \"%s\" and ner.created_at < \"%s\" and ner.effective != 0 group by n.category_id" % (day1, day2)
    # sob = mysql_tool.sql_obj()
    # rt = sob.querydb(sql, logger, sql)
    # for v in rt:
    #     news_effective_reading[int(v[0])] = int(v[1])
    # sob.closedb()

    # 获取评论数
    comments_count_hash = {}
    sql = "select n.category_id, count(*) from news_comments as nc join news as n on(nc.news_id = n.id) where nc.created_at >= \"%s\" and nc.created_at < \"%s\" group by n.category_id" % (day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        comments_count_hash[int(v[0])] = int(v[1])

    # 获取收藏数
    like_count_hash = {}
    sql = "select n.category_id, count(*) from user_like_news as uln join news as n on(uln.news_id = n.id) where uln.created_at >= \"%s\" and uln.created_at < \"%s\" group by n.category_id" % (day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        like_count_hash[int(v[0])] = int(v[1])

    # 获取点赞数, 没有用 news_comments 获取点赞，是因为用户可以在当天评论昨天或更早的文章，这样用news_comments表就无法统计了
    zan_count_hash = {}
    sql = "select n.category_id, count(*) from user_zan_news_comments as uznc join news_comments as nc on (uznc.comment_id = nc.id) join news as n on(nc.news_id = n.id) where uznc.created_at >= \"%s\" and uznc.created_at < \"%s\" group by n.category_id" % (day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        zan_count_hash[int(v[0])] = int(v[1])

    # 获取pv
    sql = "select category_id, sum(real_pv) from news group by category_id"
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        k = int(v[0])
        data[k] = {}
        data[k]['channel_name'] = u"资讯"
        data[k]['channel'] = "news"
        data[k]['category_id'] = k
        data[k]['pv_total'] = int(v[1])
        if k in news_categories.keys():
            data[k]['category_name'] = news_categories[k]
        if k in news_new_count.keys():
            data[k]['new_count'] = news_new_count[k]
        if k in news_effective_reading.keys():
            data[k]['effective_reading'] = news_effective_reading[k]
        if k in comments_count_hash.keys():
            data[k]['comments_count'] = comments_count_hash[k]
        if k in like_count_hash.keys():
            data[k]['like_count'] = like_count_hash[k]
        if k in zan_count_hash.keys():
            data[k]['zan_count'] = zan_count_hash[k]
    return data


def get_hot_news_info(nday=1, data={}):
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    tommorow = time_tool.get_someday_str(1)

    # 获取新增热点资讯
    data[HOT_NEWS_CATEGORY_ID] = {}
    sql = "select count(*) from news where hot_at >= \"%s\" and hot_at < \"%s\"" % (day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_NEWS_CATEGORY_ID]['new_count'] = int(v[0])

    # 获取热点资讯有效阅读
    # sql = "select count(*) from news_effective_readings as ner join news as n on (ner.news_id = n.id) where ner.created_at >= \"%s\" and ner.created_at < \"%s\" and n.id in (select id from news where hot_at < \"%s\") and ner.effective != 0" % (day1, day2, day2)
    # sob = mysql_tool.sql_obj()
    # rt = sob.querydb(sql, logger, sql)
    # for v in rt:
    #     data[HOT_NEWS_CATEGORY_ID]['effective_reading'] = int(v[0])
    # sob.closedb()

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
            data[category_id]['new_count'] = int(v[0])

        # 获取有效阅读
        # sql = "select count(*) from news_effective_readings as ner join news as n on (ner.news_id = n.id) where ner.created_at >= \"%s\" and ner.created_at < \"%s\" and n.id in (select news_id from %s) and ner.effective != 0" % (day1, day2, table)
        # sob = mysql_tool.sql_obj()
        # rt = sob.querydb(sql, logger, sql)
        # for v in rt:
        #     data[category_id]['effective_reading'] = int(v[0])
        # sob.closedb()

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
    sql = "select category_id, count(*) from videos where created_at >= \"%s\" and created_at < \"%s\" group by category_id" % (day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        video_new_count[int(v[0])] = int(v[1])

    # 获取有效阅读
    video_effective_reading = {}
    sql = "select v.category_id, count(*) from video_effective_readings as ver join videos as v on (ver.video_id = v.id) where ver.created_at >= \"%s\" and ver.created_at < \"%s\" and ver.effective != 0 group by v.category_id" % (day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        video_effective_reading[int(v[0])] = int(v[1])

    # 获取视频pv
    sql = "select category_id, sum(real_pv) from videos group by category_id"
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        k = int(v[0])
        data[k] = {}
        data[k]['channel_name'] = u"视频"
        data[k]['channel'] = "video"
        data[k]['category_id'] = k
        data[k]['pv_total'] = int(v[1])
        if k in video_categories.keys():
            data[k]['category_name'] = video_categories[k]
        if k in video_new_count.keys():
            data[k]['new_count'] = video_new_count[k]
        if k in video_effective_reading.keys():
            data[k]['effective_reading'] = video_effective_reading[k]

    # 获取热点视频
    data[HOT_VIDEO_CATEGORY_ID] = {}
    sql = "select count(*) from videos where hot_at >= \"%s\" and hot_at < \"%s\"" % (day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_VIDEO_CATEGORY_ID]['new_count'] = int(v[0])
    # 获取热点视频的pv
    sql = "select sum(real_pv) from news where hot_at < \"%s\"" % tommorow
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_VIDEO_CATEGORY_ID]['channel_name'] = u"视频"
        data[HOT_VIDEO_CATEGORY_ID]['channel'] = "video"
        data[HOT_VIDEO_CATEGORY_ID]['category_id'] = HOT_VIDEO_CATEGORY_ID
        data[HOT_VIDEO_CATEGORY_ID]['pv_total'] = int(v[0])
        data[HOT_VIDEO_CATEGORY_ID]['category_name'] = HOT_SHOW

    return data


def process(nday=1):
    mysql_tool.connectdb(host="47.96.238.205", database="taozuiredian-news")
    mysql_tool.querydb("SET NAMES utf8mb4")
    data_news = get_edit_news_info(nday)
    data_news = get_hot_news_info(nday, data_news)
    data_news = get_hot_list_info(nday, data_news)
    data_video = get_edit_video_info(nday)
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
