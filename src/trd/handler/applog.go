package handler

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"
	"trd/proto"
	"trd/queue"
	"trd/util"
)

var logstr = "{\"@timestamp\":\"%s\", \"ip\":\"%s\", \"ua\":\"%s\", \"token\":\"%s\", \"user_id\":\"%d\", \"from\":\"%s\", \"channel\":\"%s\", \"udid\":\"%s\", \"os\":\"%s\", \"os_version\":\"%s\", \"app_version\":\"%s\", \"app_version_code\":%d, \"model\":\"%s\", \"intranet_ip\":\"%s\", \"ssid\":\"%s\", \"network_type\":\"%s\", \"key\":\"%s\", \"group\":\"%s\", \"index\":%d, \"news_id\":%d, \"category_id\":%d, \"category_index\":%d, \"task_key\":\"%s\", \"page\":%d, \"page_limit\":%d, \"use_time\":%d, \"brand\":\"%s\", \"is_first\":\"%t\", \"is_simulator\":\"%t\", \"imei1\":\"%s\","
var logstr_single_log = "\"mac\":\"%s\", \"device_feature\":\"%s\", \"device_id\":\"%s\", \"imei2\":\"%s\", \"battery\":%d, \"idfa\":\"%s\"}"
var logstr_android = "\"mac\":\"%s\", \"device_feature\":\"%s\", \"device_id\":\"%s\", \"imei2\":\"%s\", \"custom_params\":\"%s\"}"
var logstr_ios = "\"battery\":%d, \"idfa\":\"%s\"}"

func ApplogHandler(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			b := make([]byte, 1<<16)
			n := runtime.Stack(b, false)
			// filebeat could not parse this log
			// util.Log.Error("{\"error\":\"%s\"}", b[:n])
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
	util.Log.Info(logstr+logstr_single_log, time.Now().Format("2006-01-02T15:04:05.000+08:00"),
		applog.Ip, applog.Ua, applog.Token, checkZero(applog.UserId),
		applog.From, applog.Channel, applog.Udid, applog.Os,
		applog.OsVersion, applog.AppVersion, checkZero(applog.AppVersionCode),
		applog.Model, applog.IntranetIp, applog.Ssid, applog.NetworkType,
		applog.Key, applog.Group, checkZero(applog.Index), checkZero(applog.NewsId),
		checkZero(applog.CategoryId), checkZero(applog.CategoryIndex), applog.TaskKey,
		checkZero(applog.Page), checkZero(applog.PageLimit),
		checkZero(applog.UseTime), applog.Brand, applog.IsFirst, applog.IsSimulator,
		applog.Imei1, applog.Mac, applog.DeviceFeature, applog.DeviceId, applog.Imei2,
		applog.Battery, applog.Idfa)
	w.WriteHeader(http.StatusOK)
}

func BatchlogHandler(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			b := make([]byte, 1<<16)
			n := runtime.Stack(b, false)
			// filebeat could not parse this log
			// util.Log.Error("{\"error\":\"%s\"}", b[:n])
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

	ip := batchlog.Ip
	ua, _ := url.QueryUnescape(batchlog.Ua)
	token := batchlog.Token
	user_id := checkZero(batchlog.UserId)
	from := strings.ToLower(commonData[1])
	channel := strings.ToLower(commonData[2])
	udid := commonData[3]
	os := commonData[4]
	os_version := commonData[5]
	app_version := commonData[6]
	app_version_code := str2int(commonData[7])
	model := commonData[8]
	intranet_ip := commonData[9]
	ssid := commonData[10]
	network_type := commonData[11]

	mac := ""
	brand := ""
	is_first := false
	is_simulator := false
	device_feature := ""
	device_id := ""
	imei1 := ""
	imei2 := ""
	battery := 0
	idfa := ""

	if 2 == batchlog.System {
		brand = commonData[12]
		is_first = str2bool(commonData[13])
		is_simulator = str2bool(commonData[14])
		imei1 = limitLen(commonData[15], 20)
		battery = str2int(commonData[16])
		idfa = commonData[17]

	} else {
		mac = commonData[12][:17]
		brand = commonData[13]
		is_first = str2bool(commonData[14])
		is_simulator = str2bool(commonData[15])
		device_feature = commonData[16]

		if 18 <= len(commonData) {
			device_id = limitLen(commonData[17], 32)
		}
		if 19 <= len(commonData) {
			imei1 = limitLen(commonData[18], 20)
		}
		if 20 <= len(commonData) {
			imei2 = limitLen(commonData[19], 20)
		}
	}

	for j, data := range rawData {
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
		now := time.Now()
		ms, _ := time.ParseDuration(fmt.Sprintf("%dms", j))
		now_ := now.Add(ms)
		if 2 != batchlog.System {
			custom_params := ""
			if 12 <= len(arrD) {
				custom_params = arrD[11]
			}
			util.Log.Info(logstr+logstr_android, now_.Format("2006-01-02T15:04:05.000+08:00"),
				ip, ua, token, user_id, from, channel, udid, os, os_version,
				app_version, app_version_code, model, intranet_ip, ssid,
				network_type, key, group, index, news_id, category_id,
				category_index, task_key, page, page_limit, use_time, brand,
				is_first, is_simulator, imei1, mac, device_feature, device_id,
				imei2, custom_params)
		} else {
			util.Log.Info(logstr+logstr_ios, now_.Format("2006-01-02T15:04:05.000+08:00"),
				ip, ua, token, user_id, from, channel, udid, os, os_version,
				app_version, app_version_code, model, intranet_ip, ssid,
				network_type, key, group, index, news_id, category_id,
				category_index, task_key, page, page_limit, use_time, brand,
				is_first, is_simulator, imei1, battery, idfa)
		}
	}

	w.WriteHeader(http.StatusOK)
}

func UserlogHandler(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			b := make([]byte, 1<<16)
			n := runtime.Stack(b, false)
			// filebeat could not parse this log
			// util.Log.Error("{\"error\":\"%s\"}", b[:n])
			w.Write(b[:n])
		}
	}()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		util.Log.Error("{\"error\":\"user log request:%s\"}", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("user log request:" + err.Error()))
		return
	}
	if !strings.Contains(r.Header.Get("Content-Type"), "application/json") {
		util.Log.Error("{\"error\":\"Content-Type is not application/json:%s\"}", r.Header.Get("Content-Type"))
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Content-Type is not jpplication/son:" + r.Header.Get("Content-Type")))
		return
	}
	defer r.Body.Close()
	userlog := make([]proto.Userlog, 0)
	if err := json.Unmarshal(body, &userlog); err != nil {
		util.Log.Error("{\"error\":\"json unmarshal:%s %s\"}", err.Error(), string(body))
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("json unmarshal:" + err.Error()))
		return
	}
	util.Log.Debug("userlog:%+v", userlog)
	for _, ulog := range userlog {
		ulogp := &proto.UserlogPush{}
		tm := time.Unix(int64(ulog.RegisteredAt), 0)
		timestamp := tm.Format("2006-01-02T15:04:05.000+08:00")
		ulogp.UserId = strconv.Itoa(ulog.UserId)
		ulogp.Timestamp = timestamp
		ulogp.Channel = ulog.Channel
		queue.PutInDiskQueue(ulogp)
		util.UserLog.Info("{\"@timestamp\":\"%s\",\"user_id\":\"%d\", \"channel\":\"%s\"}",
			timestamp, ulog.UserId, ulog.Channel)
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
