package blueirismqtt

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/owen97779/Pachamama-MQTT-Kafka-Producer/pkg/kafka"
	"github.com/owen97779/Pachamama-MQTT-Kafka-Producer/pkg/logger"
)

type MQTTClient struct {
	Broker         string
	Port           string
	Topic          string
	ClientID       string
	MonitorTimeout time.Duration
	Setup          *mqtt.ClientOptions
	Connection     mqtt.Client
	logger         *logger.AggregatedLogger
	Channel        chan struct{}
	bq             ByteQueue
}

type ByteQueue struct {
	sync.Mutex
	messages [][]byte
}

func (bq *ByteQueue) Enqueue(message []byte) {
	bq.Lock()
	defer bq.Unlock()
	bq.messages = append(bq.messages, message)
}

func (bq *ByteQueue) Dequeue() []byte {
	bq.Lock()
	defer bq.Unlock()
	if len(bq.messages) == 0 {
		return nil
	}
	message := bq.messages[0]
	bq.messages = bq.messages[1:]
	return message
}

func (bq *ByteQueue) Peek() []byte {
	bq.Lock()
	defer bq.Unlock()
	if len(bq.messages) == 0 {
		return nil
	}
	return bq.messages[0]
}

func (bq *ByteQueue) Len() int {
	bq.Lock()
	defer bq.Unlock()
	return len(bq.messages)
}

func NewInstance(broker, port, topic, client string,
	timeout time.Duration, log *logger.AggregatedLogger, ch chan struct{}) *MQTTClient {
	setup := mqtt.NewClientOptions()
	setup.AddBroker(fmt.Sprintf("tcp://%s:%s", broker, port))
	setup.SetClientID(client)

	connectionMade := func(client mqtt.Client) {
		log.Info("MQTT: Connected to Broker")
	}
	connectionLost := func(client mqtt.Client, err error) {
		log.Error("MQTT: Connection to Broker lost:", err)
	}
	messagePubHandler := func(client mqtt.Client, msg mqtt.Message) {
		m := fmt.Sprintf("MQTT: Message from Non-Subscribed Topic '%s': %s", msg.Topic(), msg.Payload())
		log.Info(m)
	}
	connectionReconnecting := func(mqtt.Client, *mqtt.ClientOptions) {
		log.Info("MQTT: attempting reconnection to Broker...")
	}

	setup.SetOnConnectHandler(connectionMade)
	setup.SetDefaultPublishHandler(messagePubHandler)
	setup.SetConnectionLostHandler(connectionLost)
	setup.SetReconnectingHandler(connectionReconnecting)
	setup.SetConnectRetry(true)
	setup.SetConnectRetryInterval(timeout)
	setup.SetConnectTimeout(timeout)
	setup.SetKeepAlive(timeout)

	return &MQTTClient{Broker: broker, Port: port, Topic: topic, ClientID: client,
		Setup: setup, Connection: nil, Channel: ch, logger: log}
}

func (m *MQTTClient) Connect() error {
	m.Connection = mqtt.NewClient(m.Setup)
	if token := m.Connection.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	subCallback := func(client mqtt.Client, message mqtt.Message) {
		m.bq.Enqueue(message.Payload())
		m.Channel <- struct{}{}
		m.logger.Info(fmt.Sprintf("MQTT: New Message: %s", message.Payload()))
	}
	token := m.Connection.Subscribe(m.Topic, 1, subCallback)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (m *MQTTClient) Disconnect(quiesce uint) {
	m.Connection.Disconnect(quiesce)
}

func (m *MQTTClient) Read(p []byte) (n int, err error) {
	message := m.bq.Dequeue()
	if message == nil {
		return 0, io.EOF
	}

	jsonBuff, err := m.KafkaMessageJSON(message)
	if err != nil {
		return 0, err
	}

	n = copy(p, jsonBuff)
	if n < len(jsonBuff) {
		err = fmt.Errorf("byte slice is too small to hold the message")
	}

	return n, err
}

func (m *MQTTClient) KafkaMessageJSON(rawMessage []byte) ([]byte, error) {
	// Ensure rawMessage is not empty or whitespace
	if len(bytes.TrimSpace(rawMessage)) == 0 {
		return nil, fmt.Errorf("not a valid MQTT payload to send (empty)")
	}

	// Split rawMessage into key-value pairs
	words := bytes.Fields(rawMessage)
	if len(words) < 2 {
		return nil, fmt.Errorf("not a valid MQTT payload to send (no key/value pair)")
	}

	// Construct Kafka message from key-value pairs
	kafkaMessage := kafka.Message{
		Key:       string(words[0]),
		Value:     string(bytes.Join(words[1:], []byte(" "))),
		Timestamp: time.Now(),
	}

	// Marshal Kafka message to JSON
	jsonBuff, err := json.Marshal(kafkaMessage)
	if err != nil {
		return nil, err
	}
	return jsonBuff, nil
}
