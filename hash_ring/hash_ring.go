package consistenthash

import (
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"slices"
	"sync"
)

// Global error variables which has all error types to return
var (
	ErrNoConnections = errors.New("No connected Nodes available")
	ErrNodeExits     = errors.New("Node already exists")
	ErrNodeNotFound  = errors.New("Node not found")
	ErrInHashingKey  = errors.New("Error in Hashing Key")
)

// GetIdentifier gives each CacheNode its own identity
type CacheNode interface {
	GetIdentifier() string
}

type hashRingConfig struct {
	HashFunction func() hash.Hash64
	EnableLogs   bool
}

type HashRingConfigFn func(*hashRingConfig)

func SetHashFunction(f func() hash.Hash64) HashRingConfigFn {
	return func(config *hashRingConfig) {
		config.HashFunction = f
	}
}

func EnableVerboseLogs(enabled bool) HashRingConfigFn {
	return func(config *hashRingConfig) {
		config.EnableLogs = enabled
	}
}

type HashRing struct {
	mu               sync.RWMutex
	config           hashRingConfig
	nodes            sync.Map
	sortedKeyOfNodes []int64
}

func HashRingInit(opts ...HashRingConfigFn) {
	config := &hashRingConfig{
		HashFunction: fnv.New64a,
		EnableLogs:   false,
	}
	for _, opt := range opts {
		opt(config)
	}
	return &HashRing{
		config:           *config,
		sortedKeyOfNodes: make([]int64, 0),
	}
}

// AddNode adds new Node to HashRing(for ex: adding one more db shard)
func (ring *HashRing) AddNode(node CacheNode) error {
	ring.mu.Lock()
	defer ring.mu.Unlock()

	//We find out hashVal of a node which we gonna add here
	hashVal, err := ring.generateHash(node.GetIdentifier())
	if err != nil {
		return fmt.Errorf("%w: node %s", ErrInHashingKey, node.GetIdentifier())
	}

	//Check is Node of that particular hashVal exists before, if exists return respective error type message
	if _, exists := ring.nodes.Load(hashVal); exists {
		return fmt.Errorf("%w: node %s", ErrNodeExits, node.GetIdentifier())
	}

	//Stores node at its hash position in HashRing and also record it in sortedKeyofNodes
	ring.nodes.Store(hashVal, node)
	ring.sortedKeyOfNodes = append(ring.sortedKeyOfNodes, int64(hashVal))

	//Now sort this slice of sortedKeyOfNodes
	slices.Sort(ring.sortedKeyOfNodes)

	if ring.config.EnableLogs {
		log.Printf("[HashRing] says Added Node: %s (hash: %d)", node.GetIdentifier(), hashVal)
	}
	return nil
}

//generateHash converts a string key to a uint64 hash using the configured hash function

func (ring *HashRing) generateHash(key string) (uint64, error) {
	h := ring.config.HashFunction()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}
