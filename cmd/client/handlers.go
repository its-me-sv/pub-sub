package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.AckTypeAck
	}
}

func handlerMove(gs *gamelogic.GameState, channel *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")

		switch gs.HandleMove(am) {
		case gamelogic.MoveOutComeSafe, gamelogic.MoveOutcomeSamePlayer:
			return pubsub.AckTypeAck

		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				channel,
				routing.ExchangePerilTopic,
				fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.Player.Username),
				gamelogic.RecognitionOfWar{
					Attacker: am.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("failed to declare war, error: %v\n", err)
				return pubsub.AckTypeNackRequeue
			}
			return pubsub.AckTypeAck

		default:
			return pubsub.AckTypeNackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, channel *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(row gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(row)
		log := routing.GameLog{
			CurrentTime: time.Now().UTC(),
			Username:    row.Attacker.Username,
		}

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.AckTypeNackRequeue

		case gamelogic.WarOutcomeNoUnits:
			return pubsub.AckTypeNackDiscard

		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
			log.Message = fmt.Sprintf("%s won a war against %s", winner, loser)
			if err := publishGameLog(channel, log); err != nil {
				fmt.Printf("error happend when publishing, error: %v\n", err)
				return pubsub.AckTypeNackRequeue
			}
			return pubsub.AckTypeAck

		case gamelogic.WarOutcomeDraw:
			log.Message = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			if err := publishGameLog(channel, log); err != nil {
				fmt.Printf("error happend when publishing, error: %v\n", err)
				return pubsub.AckTypeNackRequeue
			}
			return pubsub.AckTypeAck

		default:
			fmt.Println("IDK what to say, sometimes stuff happens, just ignore this msg")
			return pubsub.AckTypeNackDiscard
		}
	}
}
