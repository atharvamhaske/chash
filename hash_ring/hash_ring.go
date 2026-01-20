package consistenthash

import (
	"errors"
	"hash"
	"sync"
)

// Global error variables which has all error types to return
var (
	ErrNoConnections = errors.New("No connected Nodes available")
	ErrNodeExits     = errors.New("Node already exists")
	ErrNodeNotFound  = errors.New("Node not found")
	ErrInHashingKey  = errors.New("Error in Hashing Key")
)

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
