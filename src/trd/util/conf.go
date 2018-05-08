package util

import (
	"flag"
	"path/filepath"
	"time"
	"tripod/devkit"
	"tripod/zconf"

	l4g "tripod/3rdparty/code.google.com/p/log4go"
)

const ServiceConfigFile = "conf/trd.yaml"

var Log l4g.Logger
var UserLog l4g.Logger

var ServiceConfig struct {
	ServerLogConfigFile string
	UserLogConfigFile   string
}

var (
	rootPath      string
	TrdServerPort int
	CheckInterval time.Duration
)

func parseArgs() {
	flag.StringVar(&rootPath, "rootPath", "/opt/zyz/trd", "Root Path")
	flag.IntVar(&TrdServerPort, "trdServerPort", 9090,
		"taoredian app log collector port, default 9090")
	flag.Parse()

	if !filepath.IsAbs(rootPath) {
		p, err := filepath.Abs(rootPath)
		if err != nil {
			panic("{\"panic\":\"Convert root path to abs path failed\"}")
		}
		rootPath = p
	}
}

func init() {
	parseArgs()
	err := zconf.ParseYaml(filepath.Join(rootPath, ServiceConfigFile), &ServiceConfig)
	if err != nil {
		panic(err)
	}

	Log = devkit.NewLogger(devkit.GetAbsPath(ServiceConfig.ServerLogConfigFile, rootPath))
	UserLog = devkit.NewLogger(devkit.GetAbsPath(ServiceConfig.UserLogConfigFile, rootPath))
	Log.Info("{\"info\":\"app log server config: %+v\"}", ServiceConfig)
}
