# coding=utf-8
import requests
import json
from conf import config
from util import mysql_tool, time_tool, log_tool


# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/news_search/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "news_id":{"type":"long"}, "title":{"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_max_word"}, "content":{"type": "text", "analyzer": "ik_max_word", "search_analyzer": "ik_max_word"}}}'
logger = log_tool.logger


def get_news(nday=1):
    day1 = time_tool.get_someday_str(-nday)
    day2 = time_tool.get_someday_str(-nday+1)
    news = {}
    sql = "select id, published_at, title from news where published_at >= \"%s\" and published_at < \"%s\"" % (
        day1, day2)
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        news_id = int(v[0])
        news[news_id] = {}
        news[news_id]['@timestamp'] = str(v[1]).replace(" ", "T") + "+08:00"
        news[news_id]['news_id'] = news_id
        news[news_id]['title'] = v[2]
    return news


def get_news_content(news_id=1):
    sql = "select content from news_details where news_id = %d" % news_id
    rt = mysql_tool.querydb(sql, logger, sql)
    for v in rt:
        return v[0]

    return ""


def process(nday=0):
    mysql_tool.connectdb(host="10.100.100.198",
                         user="taozuiredian-search",
                         password="odRf4b87Q9om8AUcPry879YHAxt+",
                         database="taozuiredian-news")
    mysql_tool.querydb("SET NAMES utf8mb4")

    news = get_news(nday)
    for k, v in news.items():
        v['content'] = get_news_content(k)
        url = config.URL_NEWS_SEARCH + "/" + str(k)
        r = requests.post(url, headers=config.JSON_HEADER,
                          data=json.dumps(v), timeout=(30, 120))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error(
                "post news_research index error, status_code:%s, reason:%s", r.status_code, r.reason)

    mysql_tool.closedb()


if __name__ == '__main__':
    process()
