package handler

import (
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"trd/proto"
	"trd/util"
)

func ApplogHandler(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			b := make([]byte, 1<<16)
			n := runtime.Stack(b, false)
			util.Log.Error("%s", b[:n])
		}
	}()
	_req := r.URL.Query()
	rawData := strings.Split(_req.Get("rd"), ",")
	rawCommon, err := url.QueryUnescape(_req.Get("rc"))
	if nil == rawData || "" == rawCommon {
		util.Log.Error("rd or rc is null, req:%s", _req.Encode())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err != nil {
		util.Log.Error("request rd data error:%s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	commonData := strings.Split(rawCommon, ",")
	for i, v := range commonData {
		commonData[i] = url.QueryUnescape(v)
	}
	if 1 != commonData[0] {
		util.Log.Error("common data flag:%d", commonData[0])
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	from := commonData[1]
	channel := commonData[2]
	udid := commonData[3]
	os := commonData[4]
	os_version := commonData[5]
	app_version := commonData[6]
	app_version_code := strconv.Atoi(commonData[7])
	model := commonData[8]
	intranet_ip := commonData[9]
	ssid := commonData[10]
	network_type := common_data[11]
	mac := commonData[12]
	brand := commonData[13]

	for _, data := range rawData {
		arrD := strings.Split(data, ",")
		for i, v := range arrD {
			arrD[i] = url.QueryUnescape(v)
		}
		if 1 != arrD[0] {
			continue
		}

	}

	w.WriteHeader(http.StatusOK)
}
