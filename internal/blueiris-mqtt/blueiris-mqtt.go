package blueirismqtt

import (
	"context"
	"fmt"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	modifiedlogger "github.com/owen97779/Pachamama-BlueIris-MQTT-Producer/internal/logger"
)

type MQTTClient struct {
	Broker         string
	Port           string
	Topic          string
	ClientID       string
	MonitorTimeout time.Duration
	Setup          *mqtt.ClientOptions
	Connection     mqtt.Client
	messageAlertCh chan struct{}
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

func NewInstance(ctx context.Context, broker, port, topic, client string,
	timeout time.Duration, ch chan struct{}, log *modifiedlogger.MyLogger) *MQTTClient {
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
		Setup: setup, Connection: nil, messageAlertCh: ch}
}

func (m *MQTTClient) Connect(ctx context.Context, log *modifiedlogger.MyLogger) error {
	m.Connection = mqtt.NewClient(m.Setup)
	if token := m.Connection.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}

	subCallback := func(client mqtt.Client, message mqtt.Message) {
		m.bq.Enqueue(message.Payload())
		m.messageAlertCh <- struct{}{}
		log.Info(fmt.Sprintf("MQTT: New Message: %s", message.Payload()))
	}
	token := m.Connection.Subscribe(m.Topic, 1, subCallback)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (m *MQTTClient) Disconnect(ctx context.Context, quiesce uint) {
	m.Connection.Disconnect(quiesce)
}

func (m *MQTTClient) Read(p []byte) (n int, err error) {
	message := m.bq.Dequeue()
	if message == nil {
		err = fmt.Errorf("no message in the queue")
		return 0, err
	}

	if len(p) < len(message) {
		err = fmt.Errorf("byte slice is too small to hold the message")
		return 0, err
	}
	n = copy(p, message)

	return n, err
}
