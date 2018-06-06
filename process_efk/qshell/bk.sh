applog=`date +"applog-%Y.%m.%d" -d "-31 days"`
curl -H "Content-Type:application/json" -XPUT http://127.0.0.1:9200/_snapshot/$applog -d "{\"type\":\"fs\",\"settings\":{\"location\":\"/var/taoredian_es_bk/$applog\"}}"
curl -H "Content-Type:application/json" -XPUT http://127.0.0.1:9200/_snapshot/$applog/snapshot?wait_for_completion=true -d "{\"indices\":\"$applog\"}"

zip -r -q /var/taoredian_es_bk/$applog.zip /var/taoredian_es_bk/$applog 
mkdir -p /var/taoredian_es_bk/zip_$applog
mv /var/taoredian_es_bk/$applog.zip /var/taoredian_es_bk/zip_$applog
rm -rf /var/taoredian_es_bk/$applog

sed -i "s/replace/$applog/g" qupload.json
./qshell qupload 1 qupload.json > log/$applog.upload.log
sed -i "s/$applog/replace/g" qupload.json
