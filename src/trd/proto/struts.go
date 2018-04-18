package proto

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
	Ip             string `json:"ip"`
	Ssid           string `json:"ssid"`
	NetworkType    string `json:"network_type"`
	Mac            string `json:"mac"`
	Brand          string `json:"brand"`
	IsSimulator    bool   `json:"is_simulator"`
	DeviceFeature  string `json:"device_feature"`
	Ua             string `json:"ua"`
	Token          string `json:"token"`
	UserId         string `json:"user_id"`
	IsFirst        bool   `json:"is_first"`
}
