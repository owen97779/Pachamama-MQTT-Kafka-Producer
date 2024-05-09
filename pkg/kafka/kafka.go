// Kafka package used by Pachamama Group, uses Segmentio Kafka.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	Timeout time.Duration
	writer  *kafka.Writer
}

type KafkaConsumer struct {
	Timeout time.Duration
	reader  *kafka.Reader
}

type Message struct {
	Topic     string    `json:"topic"`
	Key       string    `json:"key"`
	Value     string    `json:"value"`
	Timestamp time.Time `json:"timestamp"`
}

/*
Context should have a timeout which will signal the time to read or write a message
*/
func NewProducer(ctx context.Context, broker, port, topic string, logger kafka.Logger, timeout time.Duration) *KafkaProducer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(broker + ":" + port),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.Hash{},
		Logger:                 logger,
		ErrorLogger:            logger,
	}

	return &KafkaProducer{
		Timeout: timeout, writer: w,
	}
}

func NewConsumer(ctx context.Context, broker, port, topic, groupID string, maxB int, logger kafka.Logger, timeout time.Duration) *KafkaConsumer {

	readerConfig := &kafka.ReaderConfig{
		Brokers:     []string{broker + ":" + port},
		Topic:       topic,
		GroupID:     groupID,
		MaxBytes:    maxB,
		Logger:      logger,
		ErrorLogger: logger}

	r := kafka.NewReader(*readerConfig)

	return &KafkaConsumer{
		Timeout: timeout, reader: r,
	}
}

/*
Write expects p[]byte to be in the form of a JSON with a
Key and Value from Message Struct.
*/
func (k *KafkaProducer) Write(p []byte) (n int, err error) {

	var msgData Message
	err = json.Unmarshal(p, &msgData)

	ctx, cancel := context.WithTimeout(context.Background(), k.Timeout)
	defer cancel()
	if err := k.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msgData.Key),
		Value: []byte(msgData.Value),
	}); err != nil {
		return 0, err
	}
	n = len(msgData.Key) + len(msgData.Value)

	return n, nil

}

/*
Read will output a JSON to P in the form of Message Struct
*/
func (k *KafkaConsumer) Read(p []byte) (n int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), k.Timeout)
	defer cancel()
	m, err := k.reader.FetchMessage(ctx)
	if ctx.Err() != nil {
		// No data to be read after context timeout (ensure timeout is long enough to read data).
		return 0, io.EOF
	}
	if err != nil {
		return 0, err
	}

	// Create an instance of MessageData and populate it with message fields
	msgData := Message{
		Topic:     m.Topic,
		Key:       string(m.Key),
		Value:     string(m.Value),
		Timestamp: m.Time,
	}

	// Marshal the MessageData into JSON format
	jsonData, err := json.Marshal(msgData)
	if err != nil {
		return 0, err
	}

	// Copy the JSON data into the provided byte slice
	n = copy(p, jsonData)
	if n < len(jsonData) {
		// Not enough space in the byte slice to copy the entire JSON data
		return n, fmt.Errorf("insufficient buffer size")
	}

	return n, nil
}

func (k *KafkaConsumer) Close() error {
	return k.reader.Close()
}
