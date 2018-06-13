package proto

type Applog struct {
	Key            string `json:"key"`
	Group          string `json:"group"`
	From           string `json:"from"`
	Channel        string `json:"channel"`
	Index          int    `json:"index"`
	NewsId         int    `json:"news_id"`
	CategoryId     int    `json:"category_id"`
	CategoryIndex  int    `json:"category_index"`
	TaskKey        string `json:"task_key"`
	Page           int    `json:"page"`
	PageLimit      int    `json:"page_limit"`
	UseTime        int    `json:"use_time"`
	Udid           string `json:"udid"`
	DeviceId       string `json:"device_id"`
	Imei1          string `json:"imei1"`
	Imei2          string `json:"imei2"`
	Os             string `json:"os"`
	OsVersion      string `json:"os_version"`
	AppVersion     string `json:"app_version"`
	AppVersionCode int    `json:"app_version_code"`
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
	UserId         int    `json:"user_id"`
	IsFirst        bool   `json:"is_first"`
	Battery        int32  `json:"battery"`
	Idfa           string `json:"idfa"`
}

type Batchlog struct {
	Rd     string `json:"rd"`
	Rc     string `json:"rc"`
	Ip     string `json:"ip"`
	Ua     string `json:"ua"`
	Token  string `json:"token"`
	UserId int    `json:"user_id"`
	System int32  `json:"system"`
}

type Userlog struct {
	UserId       int    `json:"user_id"`
	RegisteredAt int    `json:"registered_at"`
	Channel      string `json:"channel"`
}

type UserlogPush struct {
	UserId    string `json:"user_id"`
	Timestamp string `json:"@timestamp"`
	Channel   string `json:"channel"`
}
