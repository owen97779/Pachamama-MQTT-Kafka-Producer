package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	blueirismqtt "github.com/owen97779/Pachamama-MQTT-Kafka-Producer/internal/blueiris-mqtt"
	prettykafka "github.com/owen97779/Pachamama-MQTT-Kafka-Producer/internal/kafka"
	"github.com/owen97779/Pachamama-MQTT-Kafka-Producer/pkg/kafka"
	"github.com/owen97779/Pachamama-MQTT-Kafka-Producer/pkg/logger"
)

func main() {
	myLogger := logger.NewAggregatedLogger(
		log.New(os.Stdout, "[INFO]:\t", log.Ldate|log.Ltime),
		log.New(os.Stdout, "[WARN]:\t", log.Ldate|log.Ltime),
		log.New(os.Stderr, "[ERR]:\t", log.Ldate|log.Ltime))

	mqttBroker := os.Getenv("MQTT-BROKER")
	mqttPort := os.Getenv("MQTT-PORT")
	mqttTopic := os.Getenv("MQTT-TOPIC")
	mqttClientID := os.Getenv("MQTT-CLIENTID")
	mqttMonitorTimeout := 10 * time.Second
	mqttChan := make(chan struct{}, 2)

	kafkaBroker := os.Getenv("KAFKA-BROKER")
	kafkaPort := os.Getenv("KAFKA-PORT")
	KafkaTopic := os.Getenv("KAFKA-TOPIC")
	kafkaPrettyTopic := os.Getenv("KAFKA-PRETTY-TOPIC")
	kafkaTimeout := 10 * time.Second

	blueIrisMQTTConn := blueirismqtt.NewInstance(mqttBroker, mqttPort, mqttTopic,
		mqttClientID, mqttMonitorTimeout, myLogger, mqttChan)

	kafkaLoginProducer := kafka.NewProducer(kafkaBroker, kafkaPort, KafkaTopic, myLogger, kafkaTimeout)
	kafkaPrettyLoginProducer := prettykafka.NewProducer(kafkaBroker, kafkaPort, kafkaPrettyTopic, myLogger, kafkaTimeout)

	if err := blueIrisMQTTConn.Connect(); err != nil {
		myLogger.Error(err)
		os.Exit(1)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	buf := make([]byte, 200)

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
			_, err = kafkaPrettyLoginProducer.Write(buf[:n])
			if err != nil {
				myLogger.Error("KAFKA Pretty:", err)
			}
		case <-signalChan:
			/* Todo: Kafka event logging for future */
			myLogger.Warn("Shutdown event received!")
			os.Exit(0)
		}
	}

}
