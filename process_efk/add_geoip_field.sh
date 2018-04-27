#!/bin/sh

tommorow=`date -d '1 days' "+%Y.%m.%d"`
curl -XPUT "http://localhost:9200/applog-$tommorow"
curl -XPOST -H "Content-Type:application/json" "http://localhost:9200/applog-$tommorow/doc/_mapping" -d '{"properties":{"client_geoip":{"properties": {"location": {"type": "geo_point"}}}}}' 

