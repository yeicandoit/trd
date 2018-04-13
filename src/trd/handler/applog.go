package handler

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"runtime"
	"strings"
	"trd/proto"
	"trd/util"
)

const (
	KcontentType    = "Content-Type"
	ContentTypeJson = "application/json"
)

func ApplogHandler(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			b := make([]byte, 1<<16)
			n := runtime.Stack(b, false)
			util.Log.Error("[innersdk-panic]%s", b[:n])
		}
	}()
	if !strings.Contains(r.Header.Get(KcontentType), ContentTypeJson) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		util.Log.Error("read request:%s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()
	applog := &proto.Applog{}
	if err := json.Unmarshal(body, applog); err != nil {
		util.Log.Error("json unmarshal:%s %s", err.Error(), string(body))
		return
	}
	util.Log.Info("%v", applog)
	w.WriteHeader(http.StatusOK)
}
