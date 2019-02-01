package main

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"net/url"
	"strings"
)

const (
	SessionTimeout = 6000
	Offset         = "earliest"
)

type kafkaStage struct {
	logger   logger
	topic    string
	consumer kafka.Consumer
	producer kafka.Producer
}

func NewKafkaStage(l logger, url url.URL) *kafkaStage {
	servers := url.Host
	topic := strings.TrimLeft(url.Path, "/")
	randomGroupId, err := uuid.NewRandom()
	consumerConfig := kafka.ConfigMap{
		"bootstrap.servers":               servers,
		"group.id":                        randomGroupId.String(),
		"session.timeout.ms":              SessionTimeout,
		"auto.offset.reset":               Offset,
		"go.events.channel.enable":        true,
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true,
	}
	consumer, err := kafka.NewConsumer(&consumerConfig)
	l.Check(KafkaConnectionFailed, err)

	err = consumer.SubscribeTopics([]string{topic}, nil)
	l.Check(KafkaTopicSubscriptionFailed, err)

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": servers,
	})
	l.Check(KafkaConnectionFailed, err)

	return &kafkaStage{
		logger:   l,
		topic:    topic,
		consumer: *consumer,
		producer: *producer,
	}
}

func (k kafkaStage) Read(ch chan<- Message) {
	run := true

	k.logger.Info("Reading messages from Kafka topic", k.topic)

	for run == true {
		select {
		case event := <-k.consumer.Events():
			switch e := event.(type) {
			case kafka.AssignedPartitions:
				_ = k.consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				_ = k.consumer.Unassign()
			case *kafka.Message:
				ch <- Message{Id: string(e.Key), Body: string(e.Value)}
			case kafka.Error:
				k.logger.Warn("Failed to read messages from Kafka")
			case kafka.PartitionEOF:
				run = false
				close(ch)
			}
		}
	}
}

func (k kafkaStage) Write(ch <-chan Message, done chan<- bool) {
	for {
		message, more := <-ch
		if !more {
			done <- true

			return
		}

		err := k.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(message.Id),
			Value: []byte(message.Body),
		}, k.producer.Events())

		k.logger.Check(KafkaSendingFailed, err)

		e := <-k.producer.Events()
		m := e.(*kafka.Message)

		k.logger.Check(KafkaSendingFailed, m.TopicPartition.Error)
	}
}
