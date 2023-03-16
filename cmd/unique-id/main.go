package main

import (
	"fmt"
	"log"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	node.Handle("generate", func(msg maelstrom.Message) error {
		body := map[string]any{
			"type": "generate_ok",
			"id":   fmt.Sprintf("%s-%d", node.ID(), time.Now().UnixNano()),
		}

		node.Reply(msg, body)
		return nil
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
