# Pachamama MQTT Broker to Kafka Producer

This microservice acts as a bridge between our BlueIris CCTV system and our event-driven architecture at Pachamama Group. It converts incoming MQTT messages from an MQTT broker subscription into Kafka topic events.


## Functionality

1. **MQTT Topic Subscription**: Microservice connects to an MQTT broker and subscribes to a topic.
2. **MQTT Incoming Message Handling**: Processes incoming messages to a queue and signals a notification over a channel.
3. **MQTT Message Reading**: Reads message from queue and stores into a buffer.
4. **Kafka Write Message**: Writes buffered messaged to the Kafka topic.

![Alt text](usage.png "Usage")

## Usage

### Build from source
1. Clone repository
2. Export environment variables, they can be found in [docker-compose](/deploy/docker-compose.yml)
3. Create executable in /cmd/main.go

### Docker Compose
1. Fill the environment variables in [docker-compose](/deploy/docker-compose.yml).

    ```
    version: '3.8'

    services:
    kafka-producer:
        image: owen97779/pachamama-mqtt-kafka-producer
        container_name: "pachamama-mqtt-kafka-producer"
        environment:
        - MQTT-BROKER=localhost
        - MQTT-PORT=1883
        - MQTT-TOPIC=BlueIris/logins
        - MQTT-CLIENTID=Pachamama-MQTT-Kafka-Producer
        - KAFKA-BROKER=kafka0
        - KAFKA-PORT=9092
        - KAFKA-TOPIC=BlueIris-logins
        - KAFKA-PRETTY-TOPIC=BlueIris-logins-Pretty
    ```
2. Deploy container.
    ```
    docker-compose up -d
    ```

## Dependencies

- https://github.com/eclipse/paho.mqtt.golang
- https://github.com/segmentio/kafka-go