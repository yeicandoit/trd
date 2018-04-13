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

var ServiceConfig struct {
	ConfCheckInterval   int
	ServerLogConfigFile string
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
			panic("Convert root path to abs path failed")
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

	CheckInterval = time.Duration(ServiceConfig.ConfCheckInterval)
	Log = devkit.NewLogger(devkit.GetAbsPath(ServiceConfig.ServerLogConfigFile, rootPath))
	Log.Info("app log server config: %+v", ServiceConfig)
}
