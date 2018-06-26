applog=`date +"applog-%Y.%m.%d" -d "-29 days"`

curl -H "Content-Type:application/json" -XPUT http://127.0.0.1:9200/_snapshot/$applog -d "{\"type\":\"fs\",\"settings\":{\"location\":\"/var/taoredian_es_bk/$applog\"}}" > log/$applog.upload.log
grep "\"acknowledged\":true"  log/$applog.upload.log
if [ $? -ne 0 ]; then
    python myEmail.py "create $applog snapshaot failed"
    exit
fi

curl -H "Content-Type:application/json" -XPUT http://127.0.0.1:9200/_snapshot/$applog/snapshot?wait_for_completion=true -d "{\"indices\":\"$applog\"}" >> log/$applog.upload.log
grep "\"failed\":0" log/$applog.upload.log
if [ $? -ne 0 ]; then
    python myEmail.py "save $applog snapshaot failed"
    exit
fi

zip -r -q /var/taoredian_es_bk/$applog.zip /var/taoredian_es_bk/$applog 
mkdir -p /var/taoredian_es_bk/zip_$applog
mv /var/taoredian_es_bk/$applog.zip /var/taoredian_es_bk/zip_$applog
rm -rf /var/taoredian_es_bk/$applog

sed -i "s/replace/$applog/g" qupload.json
./qshell qupload 1 qupload.json >> log/$applog.upload.log
sed -i "s/$applog/replace/g" qupload.json
qshell_log=`grep "See upload log at path" log/$applog.upload.log | awk -F" " '{print $NF}'`
grep "Failure:         0" $qshell_log
if [ $? -ne 0 ]; then
    python myEmail.py "qshell upload $applog failed"
    exit
fi
rm -rf /var/taoredian_es_bk/zip_$applog
curl -XDELETE http://127.0.0.1:9200/$applog
