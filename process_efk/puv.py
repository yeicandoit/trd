import requests
import json
from datetime import datetime, timedelta
from util import time_tool
import logging.config

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/puv/doc/_mapping -d '{"properties": {"type":{"type":"keyword"},"key":{"type":"keyword"},"pv":{"type":"long"},"uv_device":{"type":"long"},"uv_ip":{"type":"long"},"@timestamp":{"type":"date"}}}'

logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_PUV_ADD = "http://localhost:9200/puv/doc"
JSON_HEADER = {"Content-Type": "application/json"}


def get_query(nday=1):
    start = time_tool.get_weehours_of_someday(nday)
    end = time_tool.get_weehours_of_someday(nday+1)
    search_data = {
        "size": 0,
        "query": {
            "bool": {
                "must": [
                    {
                        "range": {
                            "@timestamp": {
                                "gte": start * 1000,
                                "lte": end * 1000 - 1,
                                "format": "epoch_millis"
                            }
                        }
                    }
                ]
            }
        },
        "aggs": {
            "pv": {
                "terms": {
                    "field": "key.keyword",
                    "size": 50000,
                    "order": {
                        "_term": "desc"
                    }
                },
                "aggs": {
                    "uv_device": {
                        "cardinality": {
                            "field": "device_id.keyword"
                        }
                    },
                    "uv_ip": {
                        "cardinality": {
                            "field": "ip.keyword"
                        }
                    }
                }
            }
        }
    }

    return search_data


def get_puv(query_puv={}):
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query_puv), timeout=(20, 50))
    puv = {}
    if 200 == r.status_code:
        r_json = r.json()
        for v in r_json["aggregations"]["pv"]["buckets"]:
            puv[v["key"]] = {}
            puv[v["key"]]["pv"] = v["doc_count"]
            puv[v["key"]]["uv_device"] = v["uv_device"]["value"]
            puv[v["key"]]["uv_ip"] = v["uv_ip"]["value"]
    else:
        logger.error("request applog index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return puv


def set_puv(mtype, puv, someday):
    dt = datetime.today() - timedelta(someday)
    timestamp = dt.isoformat() + "+08:00"
    _id = dt.strftime('%Y-%m-%d')
    for k, v in puv.items():
        url = URL_ELASTICSEARCH_PUV_ADD + "/" + _id + "_" + mtype + "_" + k
        puv_data = {
            "@timestamp": timestamp,
            "type": mtype,
            "key": k,
            "pv": v["pv"],
            "uv_device": v["uv_device"],
            "uv_ip": v["uv_ip"]
        }
        if "%" in url:
            url = url.replace("%", "")
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(puv_data), timeout=(10, 20))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request puv index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)
            logger.error("url:%s, puv_data:%s", url, json.dumps(puv_data))


if __name__ == '__main__':
    nday = 1
    query = get_query(-nday)
    key_puv = get_puv(query)
    query["aggs"]["pv"]["terms"]["field"] = "group.keyword"
    group_puv = get_puv(query)
    set_puv("key", key_puv, nday)
    set_puv("group", group_puv, nday)
