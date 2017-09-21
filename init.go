// Copyright 2014-2016 Fraunhofer Institute for Applied Information Technology FIT

package main

import (
	"github.com/farshidtz/elog"
)

var logger *elog.Logger

func init() {
	logger = elog.New("[dgw] ", &elog.Config{
		DebugPrefix: "[dgw-debug] ",
	})
}
