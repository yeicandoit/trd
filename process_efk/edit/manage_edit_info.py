# coding=utf-8
import requests
import json
import logging.config
import edit_info
import edit_user_info
from util import mysql_tool, time_tool


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')


def process_old(nday=1):
    mysql_tool.connectdb(host="47.96.238.205", database="taozuiredian-news")
    mysql_tool.querydb("SET NAMES utf8mb4")

    data_news = edit_info.get_edit_news_info(nday)
    data_news = edit_info.get_hot_news_info(nday, data_news)
    data_news = edit_info.get_hot_list_info(nday, data_news)
    data_news = edit_info.get_news_effective_reading(nday, data_news)
    data_video = edit_info.get_edit_video_info(nday)
    data_video = edit_info.get_video_effective_reading(nday, data_video)

    mysql_tool.closedb()

    for data in [data_news, data_video]:
        for k , v in data.items():
            tid = time_tool.get_someday_str(-nday)
            url = edit_info.URL_ELASTICSEARCH_EDIT_INFO + \
                "/" + tid + "_" + v["channel"] + "_" + str(k)
            r = requests.get(url, headers=edit_info.JSON_HEADER, timeout=(30, 120))
            if 200 != r.status_code:
                print r.status_code, r.reason, r.json()
                v['@timestamp'] = time_tool.get_someday_es_format(-nday)
                r = requests.post(url, headers=edit_info.JSON_HEADER,
                                  data=json.dumps(v), timeout=(30, 120))
                if 200 != r.status_code and 201 != r.status_code:
                    print r.status_code, r.reason, r.json()
            else:
                r_json = r.json()
                if "_source" in r_json.keys():
                    td = r_json["_source"]
                    if 'effective_reading' in td.keys():
                        # td['effective_reading'] = v['effective_reading']
                        del td['effective_reading']
                print td
                # r = requests.post(url, headers=edit_info.JSON_HEADER,
                #                   data=json.dumps(td), timeout=(30, 120))
                # if 200 != r.status_code and 201 != r.status_code:
                #     print r.status_code, r.reason, r.json()


def process(nday=1):
    mysql_tool.connectdb(host="47.96.238.205", database="taozuiredian-news")
    mysql_tool.querydb("SET NAMES utf8mb4")
    data_news = edit_info.get_edit_news_info(nday)
    data_news = edit_info.get_hot_news_info(nday, data_news)
    # data_news = edit_info.get_hot_list_info(nday, data_news)
    # data_news = edit_info.get_news_effective_reading(nday, data_news)
    # data_video = edit_info.get_edit_video_info(nday)
    # data_video = edit_info.get_video_effective_reading(nday, data_video)
    mysql_tool.closedb()

    for data in [data_news]:
        for k, v in data.items():
            _id = time_tool.get_someday_str(-nday)
            v['@timestamp'] = time_tool.get_someday_es_format(-nday)
            url = "http://localhost:8200/edit_info/doc" + \
                "/" + _id + "_" + v['channel'] + "_" + str(k)
            r = requests.post(url, headers=edit_info.JSON_HEADER,
                              data=json.dumps(v), timeout=(30, 120))
            if 200 != r.status_code and 201 != r.status_code:
                logger.error("request edit index failed, status_code:%d, reason:%s, %s, %s",
                             r.status_code, json.dumps(r.json()), url, json.dumps(v))



if __name__ == '__main__':
    process()
