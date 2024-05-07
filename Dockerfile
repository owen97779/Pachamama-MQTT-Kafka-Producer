FROM golang:1.22

WORKDIR /go/src/app

# Copy only the required directories
COPY ./cmd ./cmd
COPY ./internal ./internal
COPY ./pkg ./pkg

RUN go mod download

RUN go build -o ./cmd/main ./cmd/main.go

ENV MQTT-BROKER=
ENV MQTT-PORT=
ENV MQTT-TOPIC=
ENV MQTT-CLIENTID=
ENV KAFKA-BROKER=
ENV KAFKA-PORT=
ENV KAFKA-TOPIC=
ENV KAFKA-PARTITION=

CMD [ "./cmd/main" ]