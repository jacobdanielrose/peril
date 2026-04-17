package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, ch *amqp091.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		mvOutcome := gs.HandleMove(move)
		switch mvOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.Ack
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+gs.GetUsername(),
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}

		fmt.Println("error: unknown move outcome")
		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp091.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(war gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(war)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return publishGameLog(ch,
				winner,
				loser,
				"%s won the war against %s",
				gs,
			)
		case gamelogic.WarOutcomeYouWon:
			return publishGameLog(ch,
				winner,
				loser,
				"%s won the war against %s",
				gs,
			)
		case gamelogic.WarOutcomeDraw:
			return publishGameLog(ch,
				winner,
				loser,
				"A war between %s and %s resulted in a draw",
				gs,
			)
		default:
			fmt.Println("error: unkown war outcome")
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(ch *amqp091.Channel, winner, loser, msg string, gs *gamelogic.GameState) pubsub.AckType {
	err := pubsub.PublishGob(
		ch,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+gs.GetUsername(),
		routing.GameLog{
			CurrentTime: time.Now(),
			Message:     fmt.Sprintf(msg, winner, loser),
			Username:    gs.GetUsername(),
		},
	)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return pubsub.NackRequeue
	}
	return pubsub.Ack
}
