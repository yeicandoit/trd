package main

import (
	"net/http"
	"runtime"
	"strconv"
	"trd/handler"
	"trd/util"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	port := strconv.Itoa(util.TrdServerPort)
	http.HandleFunc("/trd", handler.ApplogHandler)
	util.Log.Info("trd app log server starts listen :%s", port)
	err := http.ListenAndServe(":"+port, nil)
	util.Log.Info("%v\n", err)
}
