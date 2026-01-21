/*
Copyright (c) 2026 Atharva Mhaske

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package hashring

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

/*
HashRingConfigFn is a function type that modifies the hashRingConfig.
It is used as an option pattern to configure HashRing during initialization.
Functions like SetHashFunction and EnableVerboseLogs return HashRingConfigFn
which can be passed to HashRingInit to customize the hash ring behavior.
*/
type HashRingConfigFn func(*hashRingConfig)

/*
SetHashFunction returns a HashRingConfigFn that sets a custom hash function
for the HashRing. By default, HashRing uses fnv.New64a, but you can provide
your own hash function implementation. This is useful when you need a different
hashing algorithm or want to customize the hash distribution.
*/
func SetHashFunction(f func() hash.Hash64) HashRingConfigFn {
	return func(config *hashRingConfig) {
		config.HashFunction = f
	}
}

/*
EnableVerboseLogs returns a HashRingConfigFn that enables or disables verbose
logging for HashRing operations. When enabled, the HashRing will log operations
like adding nodes, removing nodes, and key-to-node mappings. This is useful for
debugging and monitoring the hash ring behavior.
*/
func EnableVerboseLogs(enabled bool) HashRingConfigFn {
	return func(config *hashRingConfig) {
		config.EnableLogs = enabled
	}
}

/*
HashRing represents a consistent hash ring data structure that maps keys to nodes
in a distributed system. It maintains a sorted list of node hash values and uses
binary search to efficiently find the appropriate node for any given key. The ring
supports dynamic addition and removal of nodes while maintaining consistent key-to-node
mapping. Fields:
  - mu: Read-write mutex for thread-safe concurrent access to the hash ring
  - config: Configuration settings including hash function and logging preferences
  - nodes: Thread-safe map storing nodes keyed by their hash values
  - sortedKeyOfNodes: Sorted slice of node hash values used for efficient binary search lookups
*/
type HashRing struct {
	mu               sync.RWMutex
	config           hashRingConfig
	nodes            sync.Map
	sortedKeyOfNodes []int64
}

/*
HashRingInit creates and initializes a new HashRing instance with optional configuration.
It accepts variadic HashRingConfigFn options to customize the hash ring behavior such as
setting a custom hash function or enabling verbose logs. By default, it uses fnv.New64a as
the hash function and disables logging. Returns a pointer to the initialized HashRing
ready for adding nodes and performing key-to-node lookups.
*/
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

/*
AddNode adds a new node to the HashRing. It computes the hash value of the node's
identifier and stores the node at that hash position. The node's hash is also added
to the sortedKeyOfNodes slice which is then sorted to maintain the ring structure.
If a node with the same hash already exists, it returns ErrNodeExits. This method
is thread-safe and can be used to dynamically add nodes to the hash ring (for example,
adding a new database shard to a distributed system).
*/
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

/*
GetNode retrieves the appropriate node from the HashRing for a given key. It computes
the hash value of the key and uses binary search on the sorted node hashes to find the
first node whose hash is greater than or equal to the key's hash. If no such node exists,
it wraps around to the first node in the ring (consistent hashing behavior). This method
is useful for determining which node should handle a particular key (for example, finding
which database shard to query for a given data key). Returns the node and nil error on
success, or nil and an error if no nodes are available or if the key cannot be hashed.
*/
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
	// Convert int64 back to uint64 to match the key type used when storing
	if node, ok := ring.nodes.Load(uint64(nodeHash)); ok {
		if ring.config.EnableLogs {
			log.Printf("[HashRing] Key '%s' (hash: %d) mapped to node (hash: %d)", key, hashVal, nodeHash)
		}
		return node.(CacheNode), nil
	}

	return nil, fmt.Errorf("%w: no node found for key %s", ErrNodeNotFound, key)
}

/*
RemoveNode removes an existing node from the HashRing. It computes the hash value of
the node's identifier, removes the node from the nodes map, and removes its hash from
the sortedKeyOfNodes slice. If the node does not exist, it returns ErrNodeNotFound.
This method is thread-safe and can be used to dynamically remove nodes from the hash
ring (for example, removing a database shard that is being decommissioned from a
distributed system).
*/
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

/*
binarySearch performs a binary search on the sortedKeyOfNodes slice to find the index
of the first node hash that is greater than or equal to the given key hash. This implements
the consistent hashing algorithm where keys are mapped to the first node whose hash is
greater than or equal to the key's hash. If no such node exists (meaning the key hash is
larger than all node hashes), it wraps around and returns index 0, implementing the ring
behavior. Returns the index of the target node and nil error on success, or -1 and
ErrNoConnectedNodes if the ring is empty.
*/
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

/*
generateHash converts a string key to a uint64 hash value using the configured hash
function from the HashRing's configuration. It creates a new hash instance, writes the
key bytes to it, and returns the 64-bit hash sum. This method is used internally by
AddNode, GetNode, and RemoveNode to compute hash values for node identifiers and lookup
keys. Returns the hash value and nil error on success, or 0 and an error if the hash
function fails to write the key bytes.
*/
func (ring *HashRing) generateHash(key string) (uint64, error) {
	h := ring.config.HashFunction()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0, err
	}
	return h.Sum64(), nil
}
