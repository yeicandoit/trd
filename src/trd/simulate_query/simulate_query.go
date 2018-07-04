package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/juju/ratelimit"
	"net"
	"net/http"
	"runtime"
	"time"
)

var post_data="{\"rd\":\"1%2Capp_user_profile_menu_task_click%2C%2C-1%2C-1%2C-1%2C-1%2C%2C-1%2C-1%2C-1,1%2Capp_task_show%2C%2C-1%2C-1%2C-1%2C-1%2C%2C-1%2C-1%2C-1\", \"rc\":\"1%2Capp%2C%2C746c9bee8f07bbd6dd3fe17a22ac9a04%2Candroid%2C6.0.1%2CV1.6.2%2C19%2CMI%25204LTE%2C192.168.69.22%2CDYJKX-5G%2Cwifi%2C58%253A44%253A98%253A47%253A12%253A23%2CXiaomi%2C0%2C0%2C1%252C0%252C1080*1920%252C-2%252C-2%252C,a752c441af47474e8a213daf1a0360fa,867831027381482\", \"ua\":\"PostmanRuntime/7.1.1\", \"token\":\"8viihj7JODHBrMbkXCnb7t3h4bRSEB5Xj6Yh1EsTZ6IPSkTt9brYPGrTsRlUfdjE\",\"ip\":\"172.18.0.1\",\"user_id\":19200, \"system\":1}"

var client = &http.Client{
	Transport: &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		MaxIdleConnsPerHost: 0,
	},
	Timeout: 50 * time.Millisecond,
}
var qps int
var url = "http://127.0.0.1:5678/batchlog"


func sendAndRecv(url string) {
    data := []byte(post_data)
	reqTrd, _ := http.NewRequest("POST", url, bytes.NewReader(data))
    reqTrd.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(reqTrd)
	if err != nil {
		fmt.Printf("%v, %v\n", time.Now(), err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Printf("%v, %v\n", time.Now(), resp.Status)
	}

}

func init() {
	flag.IntVar(&qps, "qps", 1, "qps")
    flag.StringVar(&url, "url", "http://127.0.0.1:5678/batchlog", "url")
    flag.Parse()
    fmt.Println("In init qps is", qps)
}

func main() {
    fmt.Println("req qps is", qps)
	runtime.GOMAXPROCS(runtime.NumCPU())
	bucket := ratelimit.NewBucket(time.Second/time.Duration(qps), int64(qps))
	fmt.Println("Start to request!!!")
	for {
		if 0 != bucket.TakeAvailable(1) {
			go sendAndRecv(url)
		}
	}
}
