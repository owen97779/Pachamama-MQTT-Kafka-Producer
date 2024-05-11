// This kafka package creates a single kafka message containing the login and logout time of an mqtt message
package prettykafka

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/owen97779/Pachamama-MQTT-Kafka-Producer/pkg/kafka"
	"github.com/owen97779/Pachamama-MQTT-Kafka-Producer/pkg/logger"
)

type producer struct {
	producer     *kafka.KafkaProducer
	loginHistory map[string]kafka.Message
	logger       *logger.AggregatedLogger
}

func NewProducer(kafkaBroker, kafkaPort, KafkaTopic string,
	myLogger *logger.AggregatedLogger, kafkaTimeout time.Duration) *producer {

	return &producer{
		producer:     kafka.NewProducer(kafkaBroker, kafkaPort, KafkaTopic, myLogger, kafkaTimeout),
		loginHistory: make(map[string]kafka.Message),
		logger:       myLogger,
	}
}

func (p *producer) Write(buff []byte) (n int, err error) {
	// Unmarshal the incoming message
	var rawMessage kafka.Message
	if err := json.Unmarshal(buff, &rawMessage); err != nil {
		return 0, err
	}

	// Check if it's a "logged out" message
	if strings.Contains(rawMessage.Value, "logged out") {
		// Get the previous "logged in" message
		previousLogin, ok := p.loginHistory[rawMessage.Key]
		if !ok {
			// If no previous "logged in" message, return without writing to Kafka
			return 0, nil
		}

		// Create a pretty message using the previous "logged in" timestamp and the current "logged out" timestamp
		prettyMessage := kafka.Message{
			Key: rawMessage.Key,
			Value: fmt.Sprintf("Logged in from %v - %v",
				previousLogin.Timestamp.Local().Format("2006-01-02 15:04:05"),
				rawMessage.Timestamp.Local().Format("15:04:05")),
		}
		buff, err := json.Marshal(prettyMessage)
		if err != nil {
			return 0, err
		}

		// Write the pretty message to Kafka
		n, err := p.producer.Write(buff)
		if err != nil {
			return n, err
		}
	}

	// If it's a "logged in" message, store it in the login history
	if strings.Contains(rawMessage.Value, "logged in") {
		p.loginHistory[rawMessage.Key] = rawMessage
	}

	return n, err
}
