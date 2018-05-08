package queue

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"time"
	"trd/proto"
	l4g "tripod/3rdparty/code.google.com/p/log4go"
	"tripod/diskqueue"
)

var ulQ *diskqueue.DiskQueue
var qlog l4g.Logger
var esUrl = "http://127.0.0.1:9200/user/doc/"
var client = &http.Client{
	Transport: &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		MaxIdleConnsPerHost: 10,
	},
	Timeout: 500 * time.Millisecond,
}

func InitUlQueue(name, dataPath string, log l4g.Logger, url string) {
	ulQ = diskqueue.NewDiskQueue(name, dataPath, 100*1024*1024, 2500, time.Second, log)
	qlog = log
	esUrl = url
	go processUserlog()
}

func PutInDiskQueue(ul *proto.UserlogPush) {
	bs, err := json.Marshal(ul)
	if err != nil {
		loge("marshal %v err %v", ul, err)
		return
	}
	ulQ.Put(bs)
}

func processUserlog() {
	ret := 0
	wait := false
	for {
		if wait {
			time.Sleep(1000 * time.Millisecond)
		}
		select {
		case msg := <-ulQ.ReadChan():
			ret = pushUserlog(msg)
			if ret != 0 {
				wait = true
			} else {
				wait = false
			}
		}
	}
}

func pushUserlog(bs []byte) int {
	userlog := proto.UserlogPush{}
	if err := json.Unmarshal(bs, &userlog); err != nil {
		loge("Unmarshal %v err %v", string(bs), err)
		return 0
	}
	url := esUrl + userlog.UserId
	reqEs, _ := http.NewRequest("POST", url, bytes.NewReader(bs))
	reqEs.Header.Set("Content-Type", "application/json")
	_, err := client.Do(reqEs)
	if err != nil {
		//put into queue again
		loge("userlog %+v err %v", userlog, err)
		PutInDiskQueue(&userlog)
		return 1
	}

	return 0
}

func logf(f string, args ...interface{}) {
	if qlog == nil {
		return
	}
	qlog.Info(f, args...)
}

func loge(f string, args ...interface{}) {
	if qlog == nil {
		return
	}
	qlog.Error(f, args...)
}
