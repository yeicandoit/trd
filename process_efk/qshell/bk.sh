curl -H "Content-Type:application/json" -XPUT http://127.0.0.1:9200/_snapshot/$1 -d "{\"type\":\"fs\",\"settings\":{\"location\":\"/var/taoredian_es_bk/$1\"}}"
curl -H "Content-Type:application/json" -XPUT http://127.0.0.1:9200/_snapshot/$1/snapshot -d "{\"indices\":\"$1\"}"
