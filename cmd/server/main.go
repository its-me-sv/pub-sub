package main

import (
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

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalln("Failed to create channel")
	}
	log.Println("Channel created successfully!!")

	gamelogic.PrintServerHelp()
	shouldBreakLoop := false
	for !shouldBreakLoop {
		words := gamelogic.GetInput()

		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause", "resume":
			{
				log.Printf("Sending \"%s\" message\n", words[0])

				isPaused := true
				if words[0] == "resume" {
					isPaused = false
				}

				err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: isPaused})
				if err != nil {
					log.Fatalf("Failed to publish message")
				}
			}
		case "quit":
			{
				log.Println("Quitting")
				shouldBreakLoop = true
			}
		default:
			{
				log.Println("I don't understand the command")
			}
		}
	}

	sigChn := make(chan os.Signal, 1)
	signal.Notify(sigChn, os.Interrupt)
	if <-sigChn == os.Interrupt {
		log.Println("Shutting down!!")
	}
}
