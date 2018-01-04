// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/farshidtz/elog"
	"log"
	"os"
)

var logger *elog.Logger

func init() {
	logger = elog.New("[dgw] ", &elog.Config{
		DebugPrefix: "[dgw-debug] ",
		DebugTrace:  elog.NoTrace,
	})

	if os.Getenv("PAHO_DEBUG") == "1" {
		w := elog.NewWriter(os.Stdout)
		paho.ERROR = log.New(w, "[paho-error] ", 0)
		paho.CRITICAL = log.New(w, "[paho-crit] ", 0)
		paho.WARN = log.New(w, "[paho-warn] ", 0)
		paho.DEBUG = log.New(w, "[paho-debug] ", 0)
	}
}
