package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLog(log routing.GameLog) pubsub.AckType {
	defer gamelogic.PrintServerHelp()
	err := gamelogic.WriteLog(log)
	if err != nil {
		fmt.Printf("failed to write log to disk, error: %v\n", err)
	}
	return pubsub.AckTypeAck
}
