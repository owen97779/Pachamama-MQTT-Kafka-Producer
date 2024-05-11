package logger

import (
	"log"
)

type AggregatedLogger struct {
	InfoLogger  *log.Logger
	WarnLogger  *log.Logger
	ErrorLogger *log.Logger
}

func NewAggregatedLogger(infoLog, warnLog, errorLog *log.Logger) *AggregatedLogger {
	return &AggregatedLogger{
		InfoLogger: infoLog, WarnLogger: warnLog, ErrorLogger: errorLog,
	}
}

func (l *AggregatedLogger) Info(v ...interface{}) {
	l.InfoLogger.Println(v...)
}

func (l *AggregatedLogger) Warn(v ...interface{}) {
	l.WarnLogger.Println(v...)
}

func (l *AggregatedLogger) Error(v ...interface{}) {
	l.ErrorLogger.Println(v...)
}

// myLogger is now a kafka logger interface with this method
func (l *AggregatedLogger) Printf(s string, v ...interface{}) {
	s = "KAFKA: " + s
	l.InfoLogger.Printf(s, v...)
}
