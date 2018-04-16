package util

import (
	"net/http"
	"strings"
)

var realIpHeaders = [2]string{"X-FORWARDED-FOR", "X-REAL-IP"}

func GetRealIp(r *http.Request) (ip string) {
	for _, k := range realIpHeaders {
		if ip = r.Header.Get(k); ip != "" {
			ss := strings.Split(ip, ",")
			for _, s := range ss {
				if ip = strings.TrimSpace(s); ip != "" && ip != "unknown" {
					return
				}
			}
		}
	}

	if ip = r.RemoteAddr; ip != "" {
		ip = strings.Split(ip, ":")[0]
		return
	}

	return "0.0.0.0"
}
