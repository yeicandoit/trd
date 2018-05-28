import requests
import json
from datetime import datetime, timedelta
from util import time_tool
import logging.config

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/signin/doc/_mapping -d '{"properties": {"key":{"type":"keyword"}, "pv":{"type":"long"}, "uv_device_id":{"type":"long"}, "uv_ip":{"type":"long"}, "@timestamp":{"type":"date"}}}'

logging.config.fileConfig('conf/log.conf')
logger = logging.getLogger('main')
URL_ELASTICSEARCH_APPLOG = "http://localhost:9200/applog-*/doc/_search"
URL_ELASTICSEARCH_SIGNIN_ADD = "http://localhost:9200/signin/doc"
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
            "signin": {
                "filters": {
                    "filters": {
                        "app_week_sign_pop_show": {
                            "query_string": {
                                "query": "app_week_sign_pop_show",
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        },
                        "app_week_sign_pop_sign_button_click": {
                            "query_string": {
                                "query": "app_week_sign_pop_sign_button_click",
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        },
                        "app_week_sign_pop_receive_reward_button_click": {
                            "query_string": {
                                "query": "app_week_sign_pop_receive_reward_button_click",
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        },
                        "app_task_week_sign_button_click": {
                            "query_string": {
                                "query": "app_task_week_sign_button_click",
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        },
                        "web_activity_invite_20180428_page_show": {
                            "query_string": {
                                "query": "web_activity_invite_20180428_page_show",
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        },
                        "web_activity_invite_20180428_page_invite_button_click": {
                            "query_string": {
                                "query": "web_activity_invite_20180428_page_invite_button_click",
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        },
                        "web_activity_invite_20180428_share_page_show": {
                            "query_string": {
                                "query": "web_activity_invite_20180428_share_page_show",
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        },
                        "web_activity_invite_20180428_share_page_in_wechat_open_button_click": {
                            "query_string": {
                                "query": "web_activity_invite_20180428_share_page_in_wechat_open_button_click",
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        },
                        "web_activity_invite_20180428_share_page_open_button_click": {
                            "query_string": {
                                "query": "web_activity_invite_20180428_share_page_open_button_click",
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        },
                        "web_activity_invite_20180428_share_page_submit_button_click": {
                            "query_string": {
                                "query": "web_activity_invite_20180428_share_page_submit_button_click",
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        },
                        "web_activity_invite_20180428_share_page_download_button_click": {
                            "query_string": {
                                "query": "web_activity_invite_20180428_share_page_download_button_click",
                                "analyze_wildcard": True,
                                "default_field": "*"
                            }
                        }
                    }
                },
                "aggs": {
                    "uv_device_id": {
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


def get_signin(query_signin={}):
    data = {}
    r = requests.post(URL_ELASTICSEARCH_APPLOG, headers=JSON_HEADER,
                      data=json.dumps(query_signin), timeout=(30, 60))
    if 200 == r.status_code:
        r_json = r.json()
        for k, v in r_json['aggregations']['signin']['buckets'].items():
            data[k] = {}
            data[k]['pv'] = v['doc_count']
            data[k]['uv_ip'] = v['uv_ip']['value']
            data[k]['uv_device_id'] = v['uv_device_id']['value']
    else:
        logger.error("request applog index failed, status_code:%d, reason:%s",
                     r.status_code, r.reason)
    return data


def add_signin(data, someday):
    dt = datetime.today() - timedelta(someday)
    _id = dt.strftime('%Y-%m-%d')
    url_ = URL_ELASTICSEARCH_SIGNIN_ADD + "/" + _id
    timestamp = dt.isoformat() + "+08:00"
    for k, v in data.items():
        signin_data = {
            "@timestamp": timestamp,
            "key": k,
            "pv": v['pv'],
            "uv_device_id": v['uv_device_id'],
            "uv_ip": v['uv_ip']
        }
        url = url_ + "_" + k
        r = requests.post(url, headers=JSON_HEADER,
                          data=json.dumps(signin_data), timeout=(10, 20))
        if 200 != r.status_code and 201 != r.status_code:
            logger.error("request signin index failed, status_code:%d, reason:%s",
                         r.status_code, r.reason)
            logger.error("url:%s, data:%s", url, json.dumps(signin_data))


if __name__ == '__main__':
    nday = 1
    data = get_signin(get_query(-nday))
    add_signin(data, nday)
