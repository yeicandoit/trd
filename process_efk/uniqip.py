import requests
import json
from util import time_tool
import logging.config


logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
JSON_HEADER = {"Content-Type": "application/json"}


def get_query():
    search_data = {
        "size": 0,
        "query": {
            "bool": {
                "must": []
            }
        },
        "aggs": {
            "uniq_ip": {
                "terms": {
                    "field": "ip.keyword",
                    "size": 5000000
                }
            }
        }
    }

    return search_data


def get_oneday_uniqip(ip_arr, nday=1):
    start = time_tool.get_weehours_of_someday(-nday)
    query_uniqip = get_query()
    for i in range(144):
        query_uniqip["query"]["bool"]["must"] = [
            {
                "range": {
                    "@timestamp": {
                        "gte": (start + i * 600) * 1000,
                        "lte": (start + (i + 1) * 600) * 1000 - 1,
                        "format": "epoch_millis"
                    }
                }
            }
        ]
        r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                          data=json.dumps(query_uniqip), timeout=(10, 20))
        if 200 == r.status_code:
            r_json = r.json()
            uniqip_arr = [v['key']
                          for v in r_json['aggregations']['uniq_ip']['buckets']]
            ip_arr = list(set(ip_arr).union(set(uniqip_arr)))
        else:
            logger.error("request applog index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)
    return ip_arr


if __name__ == '__main__':
    ip_arr = []
    for i in range(31):
        ip_arr = get_oneday_uniqip(ip_arr, i)
    print ip_arr
    print len(ip_arr)
