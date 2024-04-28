package modifiedlogger

import (
	"log"

	"github.com/owen97779/Pachamama-BlueIris-MQTT-Producer/pkg/logger"
)

type MyLogger struct {
	*logger.AggregatedLogger
}

func NewLogger(infoLog, warnLog, errorLog *log.Logger) *MyLogger {
	return &MyLogger{
		logger.NewAggregatedLogger(infoLog, warnLog, errorLog),
	}
}

// myLogger is now a kafka logger interface with this method
func (l *MyLogger) Printf(s string, v ...interface{}) {
	s = "KAFKA: " + s
	l.InfoLogger.Printf(s, v...)
}
