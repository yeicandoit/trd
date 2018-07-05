# coding=utf-8
import requests
from util import time_tool, myEmail


JSON_HEADER = {"Content-Type": "application/json"}
CHECK_DOMAIN = "http://localhost:9200/"
check_url = [
    "task_info/doc/%s_1",
    "signin/doc/%s_web_activity_invite_20180428_share_page_download_button_click",
    "remain/doc/%s_official",  # yesterday of yesterday
    "puv4channel/doc/%s_23-50_app_main_list_8_6_act_show_official",
    "puv/doc/%s_group_app_news_show_all_button_click",
    "parent_vip_info/doc/%s",
    "parent_info/doc/%s_official",
    "online/doc/%s_23-55-00",
    "new_user_info/doc/%s_official",
    "edit_user_info/doc/%s_new_news_9",
    "edit_info/doc/%s_news_12",
    "cash_info/doc/%s",
    "app-stay/doc/%s",
    "active_user_info/doc/%s_official",
    "active/doc/%s"]


def process(nday=1):
    tid = time_tool.get_someday_str(-nday)
    tid_ = time_tool.get_someday_str(-nday-1)
    failed_url = []
    for url in check_url:
        if url.find("remain") != -1:
            url = CHECK_DOMAIN+url % tid_
        else:
            url = CHECK_DOMAIN+url % tid
        r = requests.get(url, headers=JSON_HEADER, timeout=(30, 120))
        if 200 != r.status_code:
            failed_url.append(url)
        else:
            r_json = r.json()
            if False == r_json["found"]:
                failed_url.append(url)
    if len(failed_url) > 0:
        failed_url_str = u"下面数据更新失败<br>" + "<br>".join(failed_url)
        myEmail.send_email("wangqiang@optaim.com",
                           failed_url_str, subject=u"淘热点数据统计监测")


if __name__ == '__main__':
    process()
