package raft

import (
	// "log"
	log "github.com/cihub/seelog"
)

func InitSeeLog() {
	testConfig := `
	<seelog type="sync">
		<outputs formatid="common">
			<console/>
		</outputs>
		<formats>
			<format id="common" format="[%Date(2006-01-02/15:04:05.000):%LEVEL:%File(%Line)] %Msg%n"/>
		</formats>
	</seelog>`
	logger, _ := log.LoggerFromConfigAsBytes([]byte(testConfig))
	log.ReplaceLogger(logger)
}

func init() {
	InitSeeLog()
}

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Infof(format, a...)
		log.Flush()
	}
	return
}
