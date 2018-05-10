package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var logstr = "{\"@timestamp\":\"%s\", \"key\":\"%s\", \"group\":\"%s\", \"from\":\"%s\", \"channel\":\"%s\", \"index\":%s, \"news_id\":%s, \"category_id\":%s, \"category_index\":%s, \"task_key\":\"%s\", \"page\":%s, \"page_limit\":%s, \"use_time\":%s, \"udid\":\"%s\", \"device_id\":\"%s\", \"imei1\":\"%s\", \"imei2\":\"%s\", \"os\":\"%s\", \"os_version\":\"%s\", \"app_version\":\"%s\", \"app_version_code\":%s, \"model\":\"%s\", \"intranet_ip\":\"%s\", \"ip\":\"%s\", \"ssid\":\"%s\", \"network_type\":\"%s\", \"mac\":\"%s\", \"brand\":\"%s\", \"is_simulator\":\"%s\", \"device_feature\":\"%s\", \"ua\":\"%s\", \"token\":\"%s\", \"user_id\":\"%s\", \"is_first\":\"%s\"}"
var rootPath string
var filename string

func parseArgs() {
	flag.StringVar(&rootPath, "rootPath", "/opt/trd/oldlog", "Root Path")
	flag.StringVar(&filename, "filename", "data_report_logs-20180401.log", "filename")
	flag.Parse()
	if !filepath.IsAbs(rootPath) {
		p, err := filepath.Abs(rootPath)
		if err != nil {
			panic("{\"panic\":\"Convert root path to abs path failed\"}")
		}
		rootPath = p
	}
}

func main() {
	parseArgs()
	fi, err := os.Open(rootPath + "/" + filename)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fi.Close()
	fw, err := os.Create("log/" + filename)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return
	}
	defer fw.Close()

	br := bufio.NewReader(fi)
	for {
		line, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		arr := strings.Split(string(line), "\t")
		if 36 != len(arr) {
			fmt.Println(string(line))
			continue
		}
		timestamp := ""
		if "" != arr[35] {
			timestamp = strings.Replace(arr[35], " ", "T", -1) + "+08:00"
		} else if "" != arr[34] {
			timestamp = strings.Replace(arr[34], " ", "T", -1) + "+08:00"
		} else {
			fmt.Println(string(line))
			continue
		}
		is_first := "false"
		if "0" != arr[33] {
			is_first = "true"
		}
		is_simulator := "false"
		if "0" != arr[28] {
			is_simulator = "true"
		}
		info := fmt.Sprintf(logstr, timestamp, arr[1], arr[2], arr[3], arr[4], arr[5],
			arr[6], arr[7], arr[8], arr[9], arr[10], arr[11], arr[12],
			arr[13], arr[14], arr[15], arr[16], arr[17], arr[18], arr[19], arr[20],
			arr[21], arr[22], arr[23], arr[24], arr[25], arr[26], arr[27], is_simulator,
			arr[29], arr[30], arr[31], arr[32], is_first)
		fw.WriteString(info + "\n")
	}
}
