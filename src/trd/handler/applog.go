package handler

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"
	"trd/proto"
	"trd/util"
)

var logstr = "{\"@timestamp\":\"%s\", \"key\":\"%s\", \"group\":\"%s\", \"from\":\"%s\", \"channel\":\"%s\", \"index\":%d, \"news_id\":%d, \"category_id\":%d, \"category_index\":%d, \"task_key\":\"%s\", \"page\":%d, \"page_limit\":%d, \"use_time\":%d, \"udid\":\"%s\", \"device_id\":\"%s\", \"imei1\":\"%s\", \"imei2\":\"%s\", \"os\":\"%s\", \"os_version\":\"%s\", \"app_version\":\"%s\", \"app_version_code\":%d, \"model\":\"%s\", \"intranet_ip\":\"%s\", \"ip\":\"%s\", \"ssid\":\"%s\", \"network_type\":\"%s\", \"mac\":\"%s\", \"brand\":\"%s\", \"is_simulator\":\"%t\", \"device_feature\":\"%s\", \"ua\":\"%s\", \"token\":\"%s\", \"user_id\":\"%d\", \"is_first\":\"%t\"}"

func ApplogHandler(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			b := make([]byte, 1<<16)
			n := runtime.Stack(b, false)
			util.Log.Error("{\"error\":\"%s\"}", b[:n])
			w.Write(b[:n])
		}
	}()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		util.Log.Error("{\"error:\":\"read request:%s\"}", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("read request:" + err.Error()))
		return
	}
	defer r.Body.Close()
	_req, _ := url.ParseQuery(string(body))
	util.Log.Debug("_req:%+v", _req)
	applog := &proto.Applog{}
	if err := json.Unmarshal([]byte(_req.Get("report_data")), applog); err != nil {
		util.Log.Error("{\"error\":\"json unmarshal:%s %s\"}", err.Error(), string(body))
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("json unmarshal:" + err.Error()))
		return
	}
	util.Log.Info(logstr, time.Now().Format("2006-01-02T15:04:05.000+08:00"),
		applog.Key, applog.Group, applog.From, applog.Channel,
		checkZero(applog.Index), checkZero(applog.NewsId),
		checkZero(applog.CategoryId), checkZero(applog.CategoryIndex),
		applog.TaskKey, checkZero(applog.Page), checkZero(applog.PageLimit),
		checkZero(applog.UseTime), applog.Udid, applog.DeviceId, applog.Imei1,
		applog.Imei2, applog.Os, applog.OsVersion, applog.AppVersion,
		checkZero(applog.AppVersionCode), applog.Model, applog.IntranetIp,
		applog.Ip, applog.Ssid, applog.NetworkType, applog.Mac,
		applog.Brand, applog.IsSimulator, applog.DeviceFeature, applog.Ua,
		applog.Token, checkZero(applog.UserId), applog.IsFirst)
	w.WriteHeader(http.StatusOK)
}

func BatchlogHandler(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			b := make([]byte, 1<<16)
			n := runtime.Stack(b, false)
			util.Log.Error("{\"error\":\"%s\"}", b[:n])
			w.Write(b[:n])
		}
	}()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		util.Log.Error("{\"error\":\"app log request:%s\"}", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("app log request:" + err.Error()))
		return
	}
	if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		util.Log.Error("{\"error\":\"Content-Type is not application/json:%s\"}", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Content-Type is not jpplication/son:" + r.Header.Get("Content-Type")))
		return
	}
	defer r.Body.Close()
	batchlog := &proto.Batchlog{}
	if err := json.Unmarshal(body, batchlog); err != nil {
		util.Log.Error("{\"error\":\"json unmarshal:%s %s\"}", err.Error(), string(body))
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("json unmarshal:" + err.Error()))
		return
	}
	util.Log.Debug("batchlog:%+v", batchlog)
	rawData := strings.Split(batchlog.Rd, ",")
	rawCommon, err := url.QueryUnescape(batchlog.Rc)
	if nil == rawData || "" == rawCommon {
		util.Log.Error("{\"error\":\"rd or rc is null, req:%s\"}", r.URL.RawQuery)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("rd or rc is null"))
		return
	}
	if err != nil {
		util.Log.Error("{\"error\":\"request rd data:%s error:%s\"}", r.URL.RawQuery, err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("request rd data:" + err.Error()))
		return
	}
	commonData := strings.Split(rawCommon, ",")
	for i, v := range commonData {
		commonData[i], _ = url.QueryUnescape(v)
	}
	if "1" != commonData[0] {
		util.Log.Error("{\"error\":\"common data flag:%s\"}", commonData[0])
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("common data flag is not 1"))
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
	ip := batchlog.Ip
	ssid := commonData[10]
	network_type := commonData[11]
	mac := commonData[12][:17]
	brand := commonData[13]
	is_simulator := str2bool(commonData[15])
	device_feature := commonData[16]
	device_id := ""
	imei1 := ""
	imei2 := ""
	if 18 <= len(commonData) {
		device_id = limitLen(commonData[17], 32)
	}
	if 19 <= len(commonData) {
		imei1 = limitLen(commonData[18], 20)
	}
	if 20 <= len(commonData) {
		imei2 = limitLen(commonData[19], 20)
	}
	ua, _ := url.QueryUnescape(batchlog.Ua)
	token := batchlog.Token
	user_id := checkZero(batchlog.UserId)
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
		page := str2int(arrD[8])
		page_limit := str2int(arrD[9])
		use_time := str2int(arrD[10])
		util.Log.Info(logstr, time.Now().Format("2006-01-02T15:04:05.000+08:00"),
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

func checkZero(i int) int {
	if 0 == i {
		return -1
	}
	return i
}
