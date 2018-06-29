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

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/edit_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "channel":{"type":"keyword"}, "channel_name":{"type":"keyword"}, "category_id":{"type":"long"}, "category_name":{"type":"keyword"}, "pv":{"type":"long"},"pv_total":{"type":"long"}, "effective_reading":{"type":"long"}, "like_count":{"type":"long"}, "like_count_total":{"type":"long"}, "comments_count":{"type":"long"}, "comments_count_total":{"type":"long"}, "new_count":{"type":"long"}}}'


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
    sql = "select n.category_id, count(*) from news_effective_readings as ner join news as n on (ner.news_id = n.id) where ner.created_at >= \"%s\" and ner.created_at < \"%s\" and ner.effective != 0 group by n.category_id" % (day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        news_effective_reading[int(v[0])] = int(v[1])

    # 获取pv, 收藏, 评论数
    sql = "select category_id, sum(real_pv), sum(like_count), sum(comments_count) from news group by category_id"
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        k = int(v[0])
        data[k] = {}
        data[k]['channel_name'] = u"资讯"
        data[k]['channel'] = "news"
        data[k]['category_id'] = k
        data[k]['pv_total'] = int(v[1])
        data[k]['like_count_total'] = int(v[2])
        data[k]['comments_count_total'] = int(v[3])
        if k in news_categories.keys():
            data[k]['category_name'] = news_categories[k]
        if k in news_new_count.keys():
            data[k]['new_count'] = news_new_count[k]
        if k in news_effective_reading.keys():
            data[k]['effective_reading'] = news_effective_reading[k]

    # 获取新增热点资讯
    data[HOT_NEWS_CATEGORY_ID] = {}
    sql = "select count(*) from news where hot_at >= \"%s\" and hot_at < \"%s\"" % (day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_NEWS_CATEGORY_ID]['new_count'] = int(v[0])

    # 获取热点资讯的pv, 收藏, 评论数.
    sql = "select sum(real_pv), sum(like_count), sum(comments_count) from news where hot_at < \"%s\"" % tommorow
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        data[HOT_NEWS_CATEGORY_ID]['channel_name'] = u"资讯"
        data[HOT_NEWS_CATEGORY_ID]['channel'] = "news"
        data[HOT_NEWS_CATEGORY_ID]['category_id'] = HOT_NEWS_CATEGORY_ID
        data[HOT_NEWS_CATEGORY_ID]['pv_total'] = int(v[0])
        data[HOT_NEWS_CATEGORY_ID]['like_count_total'] = int(v[1])
        data[HOT_NEWS_CATEGORY_ID]['comments_count_total'] = int(v[2])
        data[HOT_NEWS_CATEGORY_ID]['category_name'] = HOT_SHOW

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
    mysql_tool.connectdb()
    mysql_tool.querydb("SET NAMES utf8mb4")
    data_news = get_edit_news_info(nday)
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
                    if "like_count_total" in yd.keys():
                        v["like_count"] = v["like_count_total"] - \
                            yd["like_count_total"]
                    if "comments_count_total" in yd.keys():
                        v["comments_count"] = v["comments_count_total"] - \
                            yd["comments_count_total"]
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
