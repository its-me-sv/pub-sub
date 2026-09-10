package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf(`failed to marshal given "val", error: %v`, err)
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        data,
		},
	)
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var result bytes.Buffer
	if err := gob.NewEncoder(&result).Encode(val); err != nil {
		return fmt.Errorf(`failed to encode in "gob", error: %v`, err)
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/gob",
			Body:        result.Bytes(),
		},
	)
}
