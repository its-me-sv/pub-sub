package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) (pubsub.AckType, error) {
	return func(ps routing.PlayingState) (pubsub.AckType, error) {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.AckTypeAck, nil
	}
}

func handlerMove(gs *gamelogic.GameState, channel *amqp.Channel) func(gamelogic.ArmyMove) (pubsub.AckType, error) {
	return func(am gamelogic.ArmyMove) (pubsub.AckType, error) {
		defer fmt.Print("> ")

		switch gs.HandleMove(am) {
		case gamelogic.MoveOutComeSafe, gamelogic.MoveOutcomeSamePlayer:
			return pubsub.AckTypeAck, nil

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
				return pubsub.AckTypeNackRequeue, err
			}
			return pubsub.AckTypeAck, nil

		default:
			return pubsub.AckTypeNackDiscard, nil
		}
	}
}

func handlerWar(gs *gamelogic.GameState, channel *amqp.Channel) func(gamelogic.RecognitionOfWar) (pubsub.AckType, error) {
	return func(row gamelogic.RecognitionOfWar) (pubsub.AckType, error) {
		defer fmt.Print("> ")

		outcome, winner, loser := gs.HandleWar(row)
		log := routing.GameLog{
			CurrentTime: time.Now().UTC(),
			Username:    row.Attacker.Username,
		}

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.AckTypeNackRequeue, nil

		case gamelogic.WarOutcomeNoUnits:
			return pubsub.AckTypeNackDiscard, nil

		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon:
			log.Message = fmt.Sprintf("{%s} won a war against {%s}", winner, loser)
			if err := publishGameLog(channel, log); err != nil {
				fmt.Printf("error happend when publishing, error: %v\n", err)
				return pubsub.AckTypeNackRequeue, nil
			}
			return pubsub.AckTypeAck, nil

		case gamelogic.WarOutcomeDraw:
			log.Message = fmt.Sprintf("A war between {%s} and {%s} resulted in a draw", winner, loser)
			if err := publishGameLog(channel, log); err != nil {
				fmt.Printf("error happend when publishing, error: %v\n", err)
				return pubsub.AckTypeNackRequeue, nil
			}
			return pubsub.AckTypeAck, nil

		default:
			fmt.Println("IDK what to say, sometimes stuff happens, just ignore this msg")
			return pubsub.AckTypeNackDiscard, nil
		}
	}
}

func publishGameLog(channel *amqp.Channel, gl routing.GameLog) error {
	return pubsub.PublishGob(
		channel,
		routing.ExchangePerilTopic,
		fmt.Sprintf("%s.%s", routing.GameLogSlug, gl.Username),
		gl,
	)
}
