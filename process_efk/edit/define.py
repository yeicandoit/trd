# coding=utf-8

# Add mapping: curl -H "Content-Type:application/json" -XPOST  http://127.0.0.1:9200/edit_info/doc/_mapping -d '{"properties": {"@timestamp":{"type":"date"}, "channel":{"type":"keyword"}, "channel_name":{"type":"keyword"}, "category_id":{"type":"long"}, "category_name":{"type":"keyword"}, "pv":{"type":"long"},"pv_total":{"type":"long"}, "effective_reading":{"type":"long"}, "like_count":{"type":"long"}, "like_count_total":{"type":"long"}, "comments_count":{"type":"long"}, "comments_count_total":{"type":"long"}, "new_count":{"type":"long"}, "zan_count":{"type":"long"}, "new_choosed_count":{"type":"long"}, "new_published_count":{"type":"long"}, "yd_choosed_count":{"type":"long"}, "old_published_percentage":{"type":"long"}, "dau_count":{"type":"long"}, "share_count":{"type":"long"}, "pv_dau":{"type":"float"}, "pv_published":{"type":"float"}, "reading_pv":{"type":"float"}, "comments_pv":{"type":"float"}, "zan_pv":{"type":"float"}, "like_pv":{"type":"float"}, "share_pv":{"type":"float"}, "interval_pub_crawl":{"type":"float"}, "interval_pub_crawl_show":{"type":"keyword"}, "old_published_percentage_f":{"type":"float"}}}'

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

