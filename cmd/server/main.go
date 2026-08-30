package main

import (
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	connStr := "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalln(err)
	}
	defer conn.Close()

	log.Println("RabbitMQ connected successfully!!")

	sigChn := make(chan os.Signal, 1)
	signal.Notify(sigChn, os.Interrupt)
	if <-sigChn == os.Interrupt {
		log.Println("Shutting down!!")
	}
}
