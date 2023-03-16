package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	node := maelstrom.NewNode()

	broadcastOK := map[string]string{"type": "broadcast_ok"}

	messages := make([]int, 0, 200)
	messageReceived := map[int]struct{}{}
	// use an RWMutex to serialize writes
	rwlock := &sync.RWMutex{}

	var topology map[string][]string

	topologyOK := map[string]string{
		"type": "topology_ok",
	}

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		var req = struct {
			Typ     string `json:"type"`
			Message int    `json:"message"`
		}{}

		err := json.Unmarshal(msg.Body, &req)
		if err != nil {
			return err
		}

		rwlock.RLock()
		_, exist := messageReceived[req.Message]
		rwlock.RUnlock()
		if exist {
			return node.Reply(msg, broadcastOK)
		}

		rwlock.Lock()
		messages = append(messages, req.Message)
		messageReceived[req.Message] = struct{}{}
		rwlock.Unlock()
		neighbors := topology[node.ID()]
		for _, ns := range neighbors {
			if ns == msg.Src {
				continue
			}

			err := node.RPC(ns, req, func(_ maelstrom.Message) error {
				return nil
			})
			if err != nil {
				log.Fatal(err)
			}
		}
		// send to another node not in neighbors to mitigate partition
	TOP:
		for n := range topology {
			if n == node.ID() {
				continue
			}
			for _, ns := range neighbors {
				if n == ns {
					continue TOP
				}
			}

			if err := node.RPC(n, req, func(_ maelstrom.Message) error {
				return nil
			}); err != nil {
				log.Fatal(err)
			}
			break
		}

		return node.Reply(msg, broadcastOK)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		readOK := map[string]any{
			"type":     "read_ok",
			"messages": messages,
		}
		return node.Reply(msg, readOK)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		top := map[string][]string{}
		var req = struct {
			Topology map[string][]string `json:"topology"`
		}{
			Topology: top,
		}

		err := json.Unmarshal(msg.Body, &req)
		if err != nil {
			return err
		}
		topology = top

		return node.Reply(msg, topologyOK)
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}
