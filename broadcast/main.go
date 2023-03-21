package main

import (
	"context"
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"runtime"
	"sync"
	"time"
)

var (
	MaxRetryCount = 3
	WorkerNum     = runtime.NumCPU()
)

type broadcastBody struct {
	Type    string  `json:"type,omitempty"`
	Message float64 `json:"message"`
}

type topologyReqBody struct {
	Type     string              `json:"type,omitempty"`
	Topology map[string][]string `json:"topology"`
}

var server *GossipServer

type broadcastWorker struct {
	id            int
	broadcastFunc func(msg maelstrom.Message, body broadcastBody)
	quitChan      chan struct{}
}

type messageHolder struct {
	msg  maelstrom.Message
	body broadcastBody
}

type workerPool []*broadcastWorker

type GossipServer struct {
	sync.RWMutex
	node       *maelstrom.Node
	messages   []int64
	messageDB  map[int64]struct{}
	queue      chan messageHolder
	workerPool workerPool
	nodeIDs    []string
}

func NewGossipServer() *GossipServer {
	server := &GossipServer{
		node:      maelstrom.NewNode(),
		messages:  make([]int64, 0),
		queue:     make(chan messageHolder),
		messageDB: make(map[int64]struct{}),
	}

	pool := make(workerPool, 0)
	for i := 0; i < WorkerNum; i++ {
		w := NewBroadcastWorker(i, server.broadCastNeighbors)
		w.start(server.queue)
		pool = append(pool, w)
	}

	server.workerPool = pool
	return server
}

func NewBroadcastWorker(idx int, handler func(message maelstrom.Message, body broadcastBody)) *broadcastWorker {
	return &broadcastWorker{
		id:            idx,
		broadcastFunc: handler,
		quitChan:      make(chan struct{}),
	}
}

func (w *broadcastWorker) start(q chan messageHolder) {
	w.quitChan = make(chan struct{})

	go func() {
		for {
			select {
			case h := <-q:
				w.broadcastFunc(h.msg, h.body)
			case <-w.quitChan:
				return
			}
		}
	}()
}

func (w *broadcastWorker) stop() {
	go func() {
		w.quitChan <- struct{}{}
	}()
}

func main() {
	server = NewGossipServer()
	server.node.Handle("read", server.readHandler)
	server.node.Handle("topology", server.topologyHandler)
	server.node.Handle("broadcast", server.broadCastHandler)

	if err := server.node.Run(); err != nil {
		fmt.Printf("Error %s", err)
	}

	for _, w := range server.workerPool {
		w.stop()
	}
}

func (g *GossipServer) broadCastHandler(msg maelstrom.Message) error {
	var body broadcastBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	var resp = map[string]any{
		"type": "broadcast_ok",
	}

	msgNum := int64(body.Message)
	if g.isMessageExist(msgNum) {
		return g.node.Reply(msg, resp)
	}

	// Debug purpose
	if WorkerNum > 0 {
		g.queue <- messageHolder{
			msg:  msg,
			body: body,
		}
	} else {
		g.broadCastNeighbors(msg, body)
	}

	g.storeMessage(msgNum)

	return g.node.Reply(msg, resp)
}

func (g *GossipServer) readHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	g.RLock()
	var resp = map[string]any{
		"messages": g.messages,
		"type":     "read_ok",
	}
	g.RUnlock()

	return g.node.Reply(msg, resp)
}

func (g *GossipServer) topologyHandler(msg maelstrom.Message) error {
	var body topologyReqBody
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	g.nodeIDs = body.Topology[g.node.ID()]
	var resp = map[string]any{
		"type": "topology_ok",
	}
	return g.node.Reply(msg, resp)
}

func (g *GossipServer) isMessageExist(msg int64) bool {
	g.RLock()
	defer g.RUnlock()

	_, ok := g.messageDB[msg]
	return ok
}

func (g *GossipServer) storeMessage(msg int64) {
	g.Lock()
	defer g.Unlock()
	g.messages = append(g.messages, msg)
	g.messageDB[msg] = struct{}{}
}

func (g *GossipServer) broadCastNeighbors(msg maelstrom.Message, body broadcastBody) {
	neighbors := g.nodeIDs

	for _, v := range neighbors {
		neighborId := v
		if neighborId == msg.Src || neighborId == g.node.ID() {
			continue
		}
		go g.broadcastWithRetries(neighborId, body)
	}
}

func (g *GossipServer) broadcastWithRetries(dst string, body broadcastBody) {
	var err error

	for i := 1; i <= MaxRetryCount; i++ {
		err = g.broadcastWithTimeout(dst, body)
		if err == nil {
			return
		}
		time.Sleep(time.Duration(i) * time.Second)
	}
}

func (g *GossipServer) broadcastWithTimeout(dst string, body broadcastBody) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err := g.node.SyncRPC(ctx, dst, body)
	return err
}
