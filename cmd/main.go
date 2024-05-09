package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	blueirismqtt "github.com/owen97779/Pachamama-MQTT-Kafka-Producer/internal/blueiris-mqtt"
	modifiedlogger "github.com/owen97779/Pachamama-MQTT-Kafka-Producer/internal/logger"
	"github.com/owen97779/Pachamama-MQTT-Kafka-Producer/pkg/kafka"
)

func main() {
	myLogger := modifiedlogger.NewLogger(
		log.New(os.Stdout, "[INFO]:\t", log.Ldate|log.Ltime),
		log.New(os.Stdout, "[WARN]:\t", log.Ldate|log.Ltime),
		log.New(os.Stderr, "[ERR]:\t", log.Ldate|log.Ltime))

	mqttBroker := os.Getenv("MQTT-BROKER")
	mqttPort := os.Getenv("MQTT-PORT")
	mqttTopic := os.Getenv("MQTT-TOPIC")
	mqttClientID := os.Getenv("MQTT-CLIENTID")
	mqttMonitorTimeout := 10 * time.Second
	mqttChan := make(chan struct{})

	kafkaBroker := os.Getenv("KAFKA-BROKER")
	kafkaPort := os.Getenv("KAFKA-PORT")
	KafkaTopic := os.Getenv("KAFKA-TOPIC")
	kafkaTimeout := 10 * time.Second

	blueIrisMQTTConn := blueirismqtt.NewInstance(context.Background(), mqttBroker, mqttPort, mqttTopic,
		mqttClientID, mqttMonitorTimeout, myLogger, mqttChan)

	kafkaLoginProducer := kafka.NewProducer(context.Background(), kafkaBroker, kafkaPort, KafkaTopic, myLogger, kafkaTimeout)

	if err := blueIrisMQTTConn.Connect(context.Background(), myLogger); err != nil {
		myLogger.Error(err)
		os.Exit(1)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	buf := make([]byte, 100)

	for {
		select {
		case <-mqttChan:
			n, err := blueIrisMQTTConn.Read(buf)
			if err != nil {
				myLogger.Error("MQTT:", err)
				continue
			}
			_, err = kafkaLoginProducer.Write(buf[:n])
			if err != nil {
				/* Todo: Enqueue the buf to BQ in BlueIrisConn */
				myLogger.Error("KAFKA:", err)
			}
		case <-signalChan:
			/* Todo: Kafka event logging for future */
			myLogger.Warn("Shutdown event received!")
			os.Exit(0)
		}
	}

}
