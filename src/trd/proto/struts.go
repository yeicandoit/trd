package proto

import "fmt"

type Applog struct {
	Key            string `json:"key"`
	Group          string `json:"group"`
	From           string `json:"from"`
	Channel        string `json:"channel"`
	Index          int32  `json:"index"`
	NewsId         int32  `json:"news_id"`
	CategoryId     int32  `json:"category_id"`
	CategoryIndex  int32  `json:"category_index"`
	TaskKey        string `json:"task_key"`
	Page           int32  `json:"page"`
	PageLimit      int32  `json:"page_limit"`
	UseTime        int32  `json:"use_time"`
	Udid           string `json:"udid"`
	DeviceId       string `json:"device_id"`
	Imei1          string `json:"imei1"`
	Imei2          string `json:"imei2"`
	Os             string `json:"os"`
	OsVersion      string `json:"os_version"`
	AppVersion     string `json:"app_version"`
	AppVersionCode int32  `json:"app_version_code"`
	Model          string `json:"model"`
	IntranetIp     string `json:"intranet_ip"`
	Mac            string `json:"mac"`
	Ssid           string `json:"ssid"`
	NetworkType    string `json:"network_type"`
	Brand          string `json:"brand"`
	IsSimulator    bool   `json:"is_simulator"`
	DeviceFeature  string `json:"device_feature"`
	IsFirst        bool   `json:"is_first"`
}

func LogFormat(alog *Applog) string {
	return fmt.Sprintf("key:%s group:%s from:%s channel:%s index:%d news_id:%d category_id:%d category_index:%d task_key:%s page:%d page_limit:%d use_time:%d udid:%s device_id:%s imei1:%s imei2:%s os:%s os_version:%s app_version:%s app_version_code:%d model:%s intranet_ip:%s mac:%s ssid:%s network_type:%s brand:%s is_simulator:%t device_feature:%s is_first:%t",
		alog.Key, alog.Group, alog.From, alog.Channel, alog.Index, alog.NewsId,
		alog.CategoryId, alog.CategoryIndex, alog.TaskKey, alog.Page,
		alog.PageLimit, alog.UseTime, alog.Udid, alog.DeviceId, alog.Imei1,
		alog.Imei2, alog.Os, alog.OsVersion, alog.AppVersion, alog.AppVersionCode,
		alog.Model, alog.IntranetIp, alog.Mac, alog.Ssid, alog.NetworkType,
		alog.Brand, alog.IsSimulator, alog.DeviceFeature, alog.IsFirst)
}

var reportData = "key:%s group:%s from:%s channel:%s index:%d news_id:%d category_id:%d category_index:%d task_key:%s page:%d page_limit:%d use_time:%d udid:%s device_id:%s imei1:%s imei2:%s os:%s os_version:%s app_version:%s app_version_code:%d model:%s intranet_ip:%s mac:%s ssid:%s network_type:%s brand:%s is_simulator:%t device_feature:%s is_first:%t"
