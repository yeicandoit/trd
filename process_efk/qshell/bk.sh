applog=`date +"applog-%Y.%m.%d" -d "-31 days"`
curl -H "Content-Type:application/json" -XPUT http://127.0.0.1:9200/_snapshot/$applog -d "{\"type\":\"fs\",\"settings\":{\"location\":\"/var/taoredian_es_bk/$applog\"}}"
curl -H "Content-Type:application/json" -XPUT http://127.0.0.1:9200/_snapshot/$applog/snapshot?wait_for_completion=true -d "{\"indices\":\"$applog\"}"

zip -r -q /var/taoredian_es_bk/$applog.zip /var/taoredian_es_bk/$applog 
sed -i "s/replace/$applog/g" qupload.json
./qshell qupload 1 qupload.json
sed -i "s/$applog/replace/g" qupload.json
