package consistenthash

import (
	"errors"
	"fmt"
	"hash"
	"hash/fnv"
	"log"
	"slices"
	"sort"
	"sync"
)

// Global error variables which has all error types to return
var (
	ErrNoConnectedNodes = errors.New("No connected Nodes available")
	ErrNodeExits        = errors.New("Node already exists")
	ErrNodeNotFound     = errors.New("Node not found")
	ErrInHashingKey     = errors.New("Error in Hashing Key")
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

func HashRingInit(opts ...HashRingConfigFn) *HashRing {
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

	// We find out hashVal of a node which we gonna add here
	hashVal, err := ring.generateHash(node.GetIdentifier())
	if err != nil {
		return fmt.Errorf("%w: node %s", ErrInHashingKey, node.GetIdentifier())
	}

	// Check is Node of that particular hashVal exists before, if exists return respective error type message
	if _, exists := ring.nodes.Load(hashVal); exists {
		return fmt.Errorf("%w: node %s", ErrNodeExits, node.GetIdentifier())
	}

	// Stores node at its hash position in HashRing and also record it in sortedKeyofNodes
	ring.nodes.Store(hashVal, node)
	ring.sortedKeyOfNodes = append(ring.sortedKeyOfNodes, int64(hashVal))

	// Now sort this slice of sortedKeyOfNodes
	slices.Sort(ring.sortedKeyOfNodes)

	if ring.config.EnableLogs {
		log.Printf("[HashRing] says Added Node: %s (hash: %d)", node.GetIdentifier(), hashVal)
	}
	return nil
}

// GetNode retrieves the appropriate Node from HashRing for a given key (for ex: finding which db shard to query)
func (ring *HashRing) GetNode(key string) (CacheNode, error) {
	ring.mu.Lock()
	defer ring.mu.Unlock()

	// We find out hashVal of a key which we gonna lookup here
	hashVal, err := ring.generateHash(key)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInHashingKey, key)
	}

	// Binary search on the sortedKeyOfNodes to find the appropriate node hash
	index, err := ring.binarySearch(int64(hashVal))
	if err != nil {
		return nil, err
	}

	// Get the node hash from sortedKeyOfNodes at the found index
	nodeHash := ring.sortedKeyOfNodes[index]

	// Load the node from HashRing using the node hash and return it
	if node, ok := ring.nodes.Load(nodeHash); ok {
		if ring.config.EnableLogs {
			log.Printf("[HashRing] Key '%s' (hash: %d) mapped to node (hash: %d)", key, hashVal, nodeHash)
		}
		return node.(CacheNode), nil
	}

	return nil, fmt.Errorf("%w: no node found for key %s", ErrNodeNotFound, key)
}

// RemoveNode removes existing Node from HashRing(for ex: removing a db shard)
func (ring *HashRing) RemoveNode(node CacheNode) error {
	ring.mu.Lock()
	defer ring.mu.Unlock()

	// We find out hashVal of a node which we gonna remove here
	hashVal, err := ring.generateHash(node.GetIdentifier())
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInHashingKey, node.GetIdentifier())
	}

	// Check is Node of that particular hashVal exists before, if not exists return respective error type message
	if _, ok := ring.nodes.LoadAndDelete(hashVal); !ok {
		return fmt.Errorf("%w: %s", ErrNodeNotFound, node.GetIdentifier())
	}

	// Find the index of this node hash in sortedKeyOfNodes using binary search
	index, err := ring.binarySearch(int64(hashVal))
	if err != nil {
		return err
	}

	// Remove the node hash from sortedKeyOfNodes slice by slicing around the index
	ring.sortedKeyOfNodes = append(ring.sortedKeyOfNodes[:index], ring.sortedKeyOfNodes[index+1:]...)

	if ring.config.EnableLogs {
		log.Printf("[HashRing] Removed node: %s (hash: %d)", node.GetIdentifier(), hashVal)
	}
	return nil
}

func (ring *HashRing) binarySearch(key int64) (int, error) {
	if len(ring.sortedKeyOfNodes) == 0 {
		return -1, ErrNoConnectedNodes
	}

	//“Find the first index wherenodeHash ≥ requestHash” pick first servernodeHash which is greater than or equal to our hashedVal of entry we are adding
	index := sort.Search(len(ring.sortedKeyOfNodes), func(i int) bool {
		return ring.sortedKeyOfNodes[i] >= key //here key is what we pass as parameter in BS (key is a int return by generateHash to lookup which node is best)
	})

	//when wherenodeHash >= fails we simply return index as zero "Means there is no server bigger than this key"
	if index == len(ring.sortedKeyOfNodes) {
		index = 0
	}
	return index, nil
}

// generateHash converts a string key to a uint64 hash using the configured hash function
func (ring *HashRing) generateHash(key string) (uint64, error) {
	h := ring.config.HashFunction()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}
