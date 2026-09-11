package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLogs() func(log routing.GameLog) pubsub.AckType {
	return func(log routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")

		err := gamelogic.WriteLog(log)
		if err != nil {
			fmt.Printf("failed to write log to disk, error: %v\n", err)
			return pubsub.AckTypeNackRequeue
		}
		return pubsub.AckTypeAck
	}
}
