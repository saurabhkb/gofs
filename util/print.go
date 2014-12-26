package util

import (
	"log"
)

var Debug = true

func SetDebug(debug bool) {
	Debug = debug
}

func P_out(s string, args ...interface{}) {
	if !Debug {
		return
	}
	log.Printf(s, args...)
}
