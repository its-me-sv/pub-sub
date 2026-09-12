package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string
type AckType string

const (
	SimpleQueueDurable   SimpleQueueType = "durable"
	SimpleQueueTransient SimpleQueueType = "transient"
)

const (
	AckTypeAck         AckType = "Ack"
	AckTypeNackRequeue AckType = "NackRequeue"
	AckTypeNackDiscard AckType = "NackDiscard"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to create channel, err: %v", err)
	}

	queue, err := channel.QueueDeclare(
		queueName,
		queueType == SimpleQueueDurable,
		queueType == SimpleQueueTransient,
		queueType == SimpleQueueTransient,
		false,
		amqp.Table{
			"x-dead-letter-exchange": routing.ExchangeDeadLetter,
		},
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to declare queue, err: %v", err)
	}

	err = channel.QueueBind(
		queue.Name,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("failed to bind queue, err: %v", err)
	}

	return channel, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	jsonUnmarshaller := func(data []byte) (T, error) {
		var msg T
		err := json.Unmarshal(data, &msg)
		return msg, err
	}
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		jsonUnmarshaller,
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	gobUnmarshaller := func(data []byte) (T, error) {
		var msg T
		err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(&msg)
		return msg, err
	}
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		gobUnmarshaller,
	)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	err = channel.Qos(10, 0, false)
	if err != nil {
		return err
	}

	delivery, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for data := range delivery {
			msg, err := unmarshaller(data.Body)
			if err == nil {
				ack := handler(msg)
				log.Println(ack)
				switch ack {
				case AckTypeAck:
					data.Ack(false)
				case AckTypeNackRequeue:
					data.Nack(false, true)
				case AckTypeNackDiscard:
					data.Nack(false, false)
				}
			} else {
				data.Nack(false, false)
			}
		}
	}()

	return nil
}
