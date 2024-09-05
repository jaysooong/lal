package logic

import (
	"errors"
	"time"

	"github.com/IBM/sarama"
)

var Address []string
var Timeout int16 //s
var TopicTS string

type KafkaProducer struct {
	producer sarama.SyncProducer
}

func NewKafkaProducer(address []string, timeOut int16, topicTS string) *KafkaProducer {

	var kp KafkaProducer
	Address = address
	Timeout = timeOut
	TopicTS = topicTS
	return &kp
}

func (kp *KafkaProducer) Init() error {

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_1

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Errors = true

	config.Producer.Return.Successes = true
	config.Producer.Timeout = time.Duration(Timeout) * time.Second

	p, err := sarama.NewSyncProducer(Address, config)
	if err != nil {
		return err
	}
	kp.producer = p
	return nil
}

func (kp *KafkaProducer) SendMessage(topic string, data []byte) error {
	if kp.producer == nil {
		return errors.New("no producer while send message")
	}
	kafkaMsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   nil,
		Value: sarama.ByteEncoder(data),
	}
	_, _, err := kp.producer.SendMessage(kafkaMsg)
	return err
}

func (kp *KafkaProducer) SendMessageTsNotify(key string, data []byte) error {
	if kp.producer == nil {
		return errors.New("no producer while send message")
	}
	kafkaMsg := &sarama.ProducerMessage{
		Topic: TopicTS,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(data),
	}
	_, _, err := kp.producer.SendMessage(kafkaMsg)
	return err
}

func (kp *KafkaProducer) Close() error {
	if kp.producer != nil {
		return kp.producer.Close()
	}
	return nil
}
