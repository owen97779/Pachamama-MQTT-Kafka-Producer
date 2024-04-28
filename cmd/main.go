package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	blueirismqtt "github.com/owen97779/Pachamama-BlueIris-MQTT-Producer/internal/blueiris-mqtt"
	modifiedlogger "github.com/owen97779/Pachamama-BlueIris-MQTT-Producer/internal/logger"
	"github.com/owen97779/Pachamama-BlueIris-MQTT-Producer/pkg/kafka"
)

func main() {
	myLogger := modifiedlogger.NewLogger(
		log.New(os.Stdout, "[INFO]:\t", log.Ldate|log.Ltime),
		log.New(os.Stdout, "[WARN]:\t", log.Ldate|log.Ltime),
		log.New(os.Stderr, "[ERROR]:\t", log.Ldate|log.Ltime))

	mqttBroker := os.Getenv("MQTT-BROKER")
	mqttPort := os.Getenv("MQTT-PORT")
	mqttTopic := os.Getenv("MQTT-TOPIC")
	mqttClientID := os.Getenv("MQTT-CLIENTID")
	mqttMonitorTimeout := 10 * time.Second
	mqttMessageAlert := make(chan struct{})

	kafkaBroker := os.Getenv("KAFKA-BROKER")
	kafkaPort := os.Getenv("KAFKA-PORT")
	KafkaTopic := os.Getenv("KAFKA-TOPIC")
	kafkaTimeout := 10 * time.Second
	kafkaPartition, err := strconv.Atoi(os.Getenv("KAFKA-PARTITION"))
	if err != nil {
		myLogger.Error(err)
		os.Exit(1)
	}
	kafkaMaxBytes, err := strconv.Atoi(os.Getenv("KAFKA-MAXBYTES"))
	if err != nil {
		myLogger.Error(err)
		os.Exit(1)
	}

	blueIrisMQTTConn := blueirismqtt.NewInstance(context.Background(), mqttBroker, mqttPort, mqttTopic,
		mqttClientID, mqttMonitorTimeout, mqttMessageAlert, myLogger)

	kafkaLoginProducer := kafka.NewProducer(context.Background(), kafkaBroker, kafkaPort, KafkaTopic,
		kafkaMaxBytes, kafkaPartition, kafkaTimeout, myLogger)

	if err := blueIrisMQTTConn.Connect(context.Background(), myLogger); err != nil {
		myLogger.Error(err)
		os.Exit(1)
	}

	/* To do:

	1. 	To handle lost Kafka connections, enqueue failed write messages along with a timestamp
		from blueIrisMQTTConn to prevent data loss.
		After reconnecting to Kafka, dequeue all enqueued messages to ensure delivery.

	2.	Handle shutdown events.

	*/

	for {
		select {
		case <-mqttMessageAlert:
			buf := make([]byte, 50)
			n, err := blueIrisMQTTConn.Read(buf)
			if err != nil {
				myLogger.Error("MQTT:", err)
			}
			_, err = kafkaLoginProducer.Write(buf[:n])
			if err != nil {
				myLogger.Error("KAFKA:", err)
			}
		}
	}

}
