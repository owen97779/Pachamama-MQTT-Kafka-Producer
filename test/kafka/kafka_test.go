package kafka

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	modifiedlogger "github.com/owen97779/Pachamama-MQTT-Kafka-Producer/internal/logger"
	"github.com/owen97779/Pachamama-MQTT-Kafka-Producer/pkg/kafka"
)

var (
	broker   = ""
	port     = ""
	topic    = ""
	maxB     = 0
	groupid  = ""
	timeout  = 0 * time.Second
	myLogger = modifiedlogger.NewLogger(
		log.New(os.Stdout, "[INFO]:\t", log.Ldate|log.Ltime),
		log.New(os.Stdout, "[WARN]:\t", log.Ldate|log.Ltime),
		log.New(os.Stderr, "[ERR]:\t", log.Ldate|log.Ltime))
)

func TestRead(t *testing.T) {
	r := kafka.NewConsumer(context.Background(), broker, port, topic, groupid, maxB, myLogger, timeout)
	buf := make([]byte, maxB)
	start := time.Now()
	n, err := r.Read(buf)
	if err != nil {
		t.Errorf("Read(buf) = %v, expected success", err)
	} else {
		t.Logf("Read: %s, time taken %v", buf[:n], time.Since(start))
	}
	r.Close()
}
