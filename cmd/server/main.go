package main

import (
	"fmt"
	"log"

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

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalln("Failed to create channel")
	}
	log.Println("Channel created successfully!!")

	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		fmt.Sprintf("%s.*", routing.GameLogSlug),
		pubsub.SimpleQueueDurable,
	)
	if err != nil {
		log.Fatalln("Failed to declare and bind queue")
	}
	log.Println("Queue declared and binded successfully!!")

	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause", "resume":
			isPaused := true
			if words[0] == "resume" {
				isPaused = false
			}

			log.Printf("Sending \"%s\" message\n", words[0])
			err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: isPaused,
				},
			)
			if err != nil {
				log.Fatalf("Failed to publish message")
			}
		case "quit":
			log.Println("Quitting")
			return
		default:
			log.Println("I don't understand the command")
		}
	}

}
