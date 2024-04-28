package kafka

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaConfig struct {
	Broker    string
	Port      string
	Topic     string
	Partition int
	MaxBytes  int
	Timeout   time.Duration
	Logger    kafka.Logger
}

type KafkaProducer struct {
	KafkaConfig
	writer *kafka.Writer
}

type KafkaConsumer struct {
	KafkaConfig
	reader *kafka.Reader
}

func NewProducer(ctx context.Context, broker, port, topic string, maxB, part int, dur time.Duration,
	logger kafka.Logger) *KafkaProducer {
	config := KafkaConfig{
		Broker: broker, Port: port, Topic: topic, Partition: part, MaxBytes: maxB, Timeout: dur,
		Logger: logger,
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(config.Broker + ":" + config.Port),
		Topic:                  config.Topic,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.Hash{},
		Logger:                 config.Logger,
	}

	return &KafkaProducer{
		KafkaConfig: config, writer: w,
	}
}

func NewConsumer(ctx context.Context, broker, port, topic string, maxB, part int, dur time.Duration,
	logger kafka.Logger) *KafkaConsumer {
	config := KafkaConfig{
		Broker: broker, Port: port, Topic: topic, Partition: part, MaxBytes: maxB, Timeout: dur,
		Logger: logger,
	}

	readerConfig := &kafka.ReaderConfig{
		Brokers:   []string{config.Broker},
		Topic:     config.Topic,
		Partition: config.Partition,
		MaxBytes:  config.MaxBytes,
		Logger:    config.Logger}

	r := kafka.NewReader(*readerConfig)

	return &KafkaConsumer{
		KafkaConfig: config, reader: r,
	}
}

// Satisfies io.Reader and Writer
// First word in p will be the key, " " indicates no key value
func (k *KafkaProducer) Write(p []byte) (n int, err error) {
	words := bytes.Fields(p)
	if len(words) == 0 {
		return 0, fmt.Errorf("no data in buffer to send")
	}
	key := words[0]
	val := bytes.Join(words[1:], []byte(" "))

	if bytes.Contains(key, []byte(" ")) {
		key = nil
	}

	kafkaMessage := kafka.Message{Key: key, Value: val}
	ctx, cancel := context.WithTimeout(context.Background(), k.Timeout)
	defer cancel()
	if err = k.writer.WriteMessages(ctx, kafkaMessage); err != nil {
		return 0, err
	}

	return n, nil

}

func (k *KafkaConsumer) Read(p []byte) (n int, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), k.Timeout)
	defer cancel()

	m, err := k.reader.ReadMessage(ctx)
	if err != nil {
		return
	}

	maxCopy := len(p)
	n = copy(p, m.Key)
	if n >= maxCopy {
		return n, err
	}
	n += copy(p[n:], m.Value)

	return n, err
}
