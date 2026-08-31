package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalln("Failed to get username")
	}

	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		log.Fatalln("Failed to declare and bind queue")
	}

	sigChn := make(chan os.Signal, 1)
	signal.Notify(sigChn, os.Interrupt)
	if <-sigChn == os.Interrupt {
		log.Println("Shutting down!!")
	}
}
