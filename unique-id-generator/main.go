package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"strconv"
	"sync"
	"time"
)

const (
	SnowFlake = 1
	UUID      = 2
)

// Snowflake is a structure that holds the necessary information for generating IDs
type Snowflake struct {
	mutex      sync.Mutex
	lastStamp  int64
	workerId   int64
	datacenter int64
}

// NewSnowflake creates a new Snowflake instance with the given worker ID and datacenter ID
func NewSnowflake(workerId int64, datacenter int64) *Snowflake {
	return &Snowflake{
		lastStamp:  0,
		workerId:   workerId,
		datacenter: datacenter,
	}
}

// NextId generates a new Snowflake ID
func (sf *Snowflake) NextId() (int64, error) {
	sf.mutex.Lock()
	defer sf.mutex.Unlock()

	timestamp := time.Now().UnixNano() / int64(time.Millisecond)

	if timestamp < sf.lastStamp {
		return 0, errors.New("time is moving backwards, waiting until a new millisecond")
	}

	if timestamp == sf.lastStamp {
		sf.workerId++
		if sf.workerId > 0xfff {
			sf.workerId = 0
			for timestamp <= sf.lastStamp {
				timestamp = time.Now().UnixNano() / int64(time.Millisecond)
			}
		}
	} else {
		sf.workerId = 0
	}

	sf.lastStamp = timestamp

	id := ((timestamp - 1288834974657) << 22) |
		(sf.datacenter << 17) |
		(sf.workerId << 12)

	return id, nil
}

type Generator interface {
	GenerateId() (string, error)
}

type DistributedIdGenerator struct {
	Generator
	mode      int
	nodeId    string
	snowflake *Snowflake
	uuid      uuid.UUID
}

var generator *DistributedIdGenerator
var node *maelstrom.Node

func NewDistributedIdGenerator(mode int, nodeId string) *DistributedIdGenerator {
	nodeIdNum, err := strconv.ParseInt(nodeId, 10, 64)
	var snowflake *Snowflake

	if err != nil {
		fmt.Printf("Error while parsing nodeId: %s", err)
		snowflake = nil
	} else {
		snowflake = NewSnowflake(nodeIdNum, nodeIdNum)
	}

	return &DistributedIdGenerator{
		mode:      mode,
		nodeId:    nodeId,
		snowflake: snowflake,
		uuid:      uuid.New(),
	}
}

func (g *DistributedIdGenerator) generateIdSnowflake() (string, error) {
	id, err := g.snowflake.NextId()
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(id, 10), nil
}

func (g *DistributedIdGenerator) generateIdUUID() (string, error) {
	return g.uuid.String() + node.ID(), nil
}

func (g *DistributedIdGenerator) GenerateId() (string, error) {
	switch g.mode {
	case SnowFlake:
		return g.generateIdSnowflake()
	default:
		return g.generateIdUUID()
	}
}

func initHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	generator = NewDistributedIdGenerator(SnowFlake, body["node_id"].(string)[1:])
	return nil
}

func generateHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	id, err := generator.GenerateId()
	if err != nil {
		fmt.Printf("Error while generating id: %s", err)
	}

	body["type"] = "generate_ok"
	body["id"] = id

	return node.Reply(msg, body)
}

func main() {
	node = maelstrom.NewNode()

	node.Handle("init", initHandler)
	node.Handle("generate", generateHandler)

	if err := node.Run(); err != nil {
		fmt.Printf("Error: %s", err)
		panic(err)
	}
}
