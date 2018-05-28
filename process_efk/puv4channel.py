import requests
import json
import time
import copy
from util import time_tool
import logging.config

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/puv4channel/doc/_mapping -d '{"properties": {"channel":{"type":"keyword"},"key":{"type":"keyword"},"pv":{"type":"long"},"uv":{"type":"long"},"@timestamp":{"type":"date"}}}'

logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_PUV4CHANNEL_ADD = "http://localhost:9200/puv4channel/doc"
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
            "pv": {
                "terms": {
                    "field": "key.keyword",
                    "size": 50000,
                    "order": {
                        "_term": "desc"
                    }
                },
                "aggs": {
                    "channel_pv": {
                        "terms": {
                            "field": "channel.keyword",
                            "size": 50000
                        }
                    }
                }
            }
        }
    }

    return search_data


def get_query_uv():
    search_data = {
        "size": 0,
        "query": {
            "bool": {
                "must": [],
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
        },
        "aggs": {
            "uv": {
                "terms": {
                    "field": "key.keyword",
                    "size": 50000,
                    "order": {
                        "_term": "desc"
                    }
                },
                "aggs": {
                    "channel": {
                        "terms": {
                            "field": "channel.keyword",
                            "size": 50000
                        },
                        "aggs": {
                            "channel_uv": {
                                "cardinality": {
                                    "field": "user_id.keyword"
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    return search_data


def get_puv4channel(query_puv4channel={}):
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query_puv4channel), timeout=(10, 20))
    if 200 == r.status_code:
        r_json = r.json()
        return r_json
    else:
        logger.error("request applog index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return {}


def set_puv4channel(data):
    for k, v in data.items():
        url = URL_ELASTICSEARCH_PUV4CHANNEL_ADD + "/" + k
        puv4channel_data = {
            "@timestamp": v["@timestamp"],
            "key": v["key"],
            "channel": v["channel"],
        }
        if "pv" in v.keys():
            puv4channel_data["pv"] = v["pv"]
        if "uv" in v.keys():
            puv4channel_data["uv"] = v["uv"]
        if "%" in url:
            url = url.replace("%", "")
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(puv4channel_data), timeout=(30, 60))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request puv4channel index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)
            logger.error("url:%s, puv4channel_data:%s",
                         url, json.dumps(puv4channel_data))


def process(query={}, query_uv={}, nday=1):
    start = time_tool.get_weehours_of_someday(-nday)
    for i in range(144):  # calculate every 10 minutes
        data = {}
        timestamp = start + (i + 1) * 600
        time_array = time.localtime(timestamp)
        time_str = time.strftime("%Y-%m-%dT%H:%M:%S+08:00", time_array)
        hkey = time.strftime("%Y-%m-%d_%H-%M", time_array)
        query["query"]["bool"]["must"] = [
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
        r_json = get_puv4channel(query)
        for key_channel in r_json['aggregations']['pv']['buckets']:
            hkey_ = hkey + "_" + key_channel['key']
            for channel in key_channel['channel_pv']['buckets']:
                hkey__ = hkey_ + "_" + channel['key']
                data[hkey__] = {}
                data[hkey__]['pv'] = channel['doc_count']
                data[hkey__]['key'] = key_channel['key']
                data[hkey__]['channel'] = channel['key']
                data[hkey__]['@timestamp'] = time_str
        query_uv["query"]["bool"]["must"] = [
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
        r_json = get_puv4channel(query_uv)
        for key_channel in r_json['aggregations']['uv']['buckets']:
            hkey_ = hkey + "_" + key_channel['key']
            for channel in key_channel['channel']['buckets']:
                hkey__ = hkey_ + "_" + channel['key']
                if hkey__ not in data.keys():
                    data[hkey__] = {}
                data[hkey__]['uv'] = channel['channel_uv']['value']
                data[hkey__]['key'] = key_channel['key']
                data[hkey__]['channel'] = channel['key']
                data[hkey__]['@timestamp'] = time_str
        set_puv4channel(data)


if __name__ == '__main__':
    for nday in range(42, 55):
        if 0 == nday or 2 == nday:
            continue
        query = get_query()
        query_uv = get_query_uv()
        process(copy.deepcopy(query), copy.deepcopy(query_uv), nday)
        query["aggs"]["pv"]["terms"]["field"] = "group.keyword"
        query_uv["aggs"]["uv"]["terms"]["field"] = "group.keyword"
        process(copy.deepcopy(query), copy.deepcopy(query_uv), nday)
