package main

import (
	"crfs/common"
	"crfs/master"
)

func main() {
	done := make(chan bool)
	m := master.MakeMaster("/data", common.CleanupInterval, common.ExpiredDuration, done)

	go common.Listen(m, 9999, "master")
	<-done
}
