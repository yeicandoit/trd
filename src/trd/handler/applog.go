package handler

import (
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
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
		commonData[i], _ = url.QueryUnescape(v)
	}
	if "1" != commonData[0] {
		util.Log.Error("common data flag:%d", commonData[0])
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	from := strings.ToLower(commonData[1])
	channel := strings.ToLower(commonData[2])
	udid := commonData[3]
	os := commonData[4]
	os_version := commonData[5]
	app_version := commonData[6]
	app_version_code := str2int(commonData[7])
	model := commonData[8]
	intranet_ip := commonData[9]
	// TODO what does api->ip() mean in ApiHelper.php?
	ip := util.GetRealIp(r)
	ssid := commonData[10]
	network_type := commonData[11]
	mac := commonData[12][:17]
	brand := commonData[13]
	is_simulator := str2bool(commonData[15])
	device_feature := commonData[16]
	device_id := limitLen(commonData[17], 32)
	imei1 := limitLen(commonData[18], 20)
	imei2 := limitLen(commonData[19], 20)
	ua := r.UserAgent()
	token := func() string {
		if "" != r.Header.Get("token") {
			return r.Header.Get("token")
		}
		return _req.Get("token")
	}()
	// TODO how to get userid?
	user_id := ""
	is_first := str2bool(commonData[14])

	for _, data := range rawData {
		data_decode, _ := url.QueryUnescape(data)
		arrD := strings.Split(data_decode, ",")
		for i, v := range arrD {
			arrD[i], _ = url.QueryUnescape(v)
		}
		if "1" != arrD[0] {
			continue
		}

		key := strings.ToLower(arrD[1])
		group := strings.ToLower(arrD[2])
		index := str2int(arrD[3])
		news_id := str2int(arrD[4])
		category_id := str2int(arrD[5])
		category_index := str2int(arrD[6])
		task_key := arrD[7]
		page := str2int(arrD[8]) // TODO check the logic
		page_limit := str2int(arrD[9])
		use_time := str2int(arrD[10])
		util.Log.Info("key:%s, group:%s, from:%s, channel:%s, index:%d, news_id:%d, category_id:%d, category_index:%d, task_key:%s, page:%d, page_limit:%d, use_time:%d, udid:%s, device_id:%s, imei1:%s, imei2:%s, os:%s, os_version:%s, app_version:%s, app_version_code:%d, model:%s, intranet_ip:%s, ip:%s, ssid:%s, network_type:%s, mac:%s, brand:%s, is_simulator:%t, device_feature:%s, ua:%s, token:%s, user_id:%s, is_first:%t",
			key, group, from, channel, index, news_id, category_id,
			category_index, task_key, page, page_limit, use_time, udid,
			device_id, imei1, imei2, os, os_version, app_version,
			app_version_code, model, intranet_ip, ip, ssid, network_type, mac,
			brand, is_simulator, device_feature, ua, token, user_id, is_first)
	}

	w.WriteHeader(http.StatusOK)
}

func str2bool(i string) bool {
	if "1" == i {
		return true
	}
	return false
}

func str2int(s string) int {
	if "0" == s {
		return 0
	}
	t, _ := strconv.Atoi(s)
	if 0 == t {
		return -1
	}
	return t
}

func limitLen(s string, length int) string {
	if length < len(s) {
		return ""
	} else {
		return s
	}
}
