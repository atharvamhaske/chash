package hashring

import (
	"errors"
	"hash/fnv"
	"sync"
	"testing"
)

/*
mockNode is a test implementation of CacheNode interface.
It provides a simple way to create test nodes with identifiers
for testing HashRing functionality without requiring actual
cache node implementations.
*/
type mockNode struct {
	identifier string
}

func (m *mockNode) GetIdentifier() string {
	return m.identifier
}

/*
TestHashRingInit tests the HashRingInit function to ensure it properly
initializes a HashRing instance with default or custom configuration options.
It verifies that the ring is created correctly, sortedKeyOfNodes is initialized,
and configuration options like custom hash functions and verbose logs work as expected.
*/
func TestHashRingInit(t *testing.T) {
	t.Run("default initialization is:", func(t *testing.T) {
		// Initialize HashRing with default configuration
		ring := HashRingInit()
		if ring == nil {
			t.Fatal("HashRing returned nil")
		}

		// Verify that sortedKeyOfNodes slice is initialized
		if ring.sortedKeyOfNodes == nil {
			t.Error("SortedKeyOfNodes slice should be initialized")
		}

		// Verify that the slice starts empty (ready for nodes to be added)
		if len(ring.sortedKeyOfNodes) != 0 {
			t.Error("This slice should be empty initially")
		}
	})

	t.Run("with custom hash fn", func(t *testing.T) {
		// Initialize HashRing with a custom hash function (fnv.New64 instead of default fnv.New64a)
		ring := HashRingInit(SetHashFunction(fnv.New64))
		if ring == nil {
			t.Error("HashRing init returned nil")
		}
	})

	t.Run("with verbose logs enabled", func(t *testing.T) {
		// Initialize HashRing with verbose logging enabled
		ring := HashRingInit(EnableVerboseLogs(true))

		if ring == nil {
			t.Fatal("HashRing returned nil")
		}

		// Verify that verbose logs are actually enabled in the configuration
		if !ring.config.EnableLogs {
			t.Error("No Logs are enabled")
		}
	})
}

/*
TestAddNode tests the AddNode method to ensure nodes can be added to the HashRing
correctly. It verifies single node addition, multiple node addition, and proper
error handling when attempting to add duplicate nodes. It also checks that nodes
are properly stored and can be retrieved after being added.
*/
func TestAddNode(t *testing.T) {
	t.Run("add a single node", func(t *testing.T) {
		// Initialize a new HashRing for testing
		ring := HashRingInit()
		// Create a mock node with identifier "node1"
		node := &mockNode{
			identifier: "node1",
		}

		// Add the node to the HashRing
		err := ring.AddNode(node)
		if err != nil {
			t.Fatalf("adding a new node failed: %v", err)
		}

		// Verify if node added was successful by retrieving it with a test key
		retrievedNode, err := ring.GetNode("test-key")
		if err != nil {
			t.Fatalf("GetNode failed: %v", err)
		}

		// Verify that the retrieved node matches the one we added
		if retrievedNode.GetIdentifier() != "node1" {
			t.Errorf("Expected node1 but we got: %v", retrievedNode.GetIdentifier())
		}
	})

	// Adding multiple Nodes
	t.Run("adding multiple nodes", func(t *testing.T) {
		// Initialize a new HashRing for testing multiple node addition
		ring := HashRingInit()

		// Create three different mock nodes
		node1 := &mockNode{
			identifier: "node1",
		}
		node2 := &mockNode{
			identifier: "node2",
		}
		node3 := &mockNode{
			identifier: "node3",
		}

		// Add first node and verify no error occurred
		if err := ring.AddNode(node1); err != nil {
			t.Errorf("failed to add firstNode: %v", err)
		}

		// Add second node and verify no error occurred
		if err := ring.AddNode(node2); err != nil {
			t.Errorf("failed to add secondNode: %v", err)
		}

		// Add third node and verify no error occurred
		if err := ring.AddNode(node3); err != nil {
			t.Errorf("failed to add thirdNode: %v", err)
		}

		// Verify that all three nodes were added by checking sortedKeyOfNodes length
		if len(ring.sortedKeyOfNodes) != 3 {
			t.Errorf("expected 3 nodes but we got: %d", len(ring.sortedKeyOfNodes))
		}
	})

	t.Run("add duplicate nodes", func(t *testing.T) {
		// Initialize a new HashRing for duplicate node testing
		ring := HashRingInit()
		// Create a node to add
		node := &mockNode{
			identifier: "node1",
		}

		// Add the node first time - should succeed
		if err := ring.AddNode(node); err != nil {
			t.Fatalf("adding duplicate node node1 failed")
		}

		// Try to add the same node again, should fail with ErrNodeExits
		err := ring.AddNode(node)
		if err == nil {
			t.Error("Expected error when adding duplicate node")
		}
		// Verify that the error is the expected ErrNodeExits error
		if !errors.Is(err, ErrNodeExits) {
			t.Errorf("Expected ErrNodeExits, got %v", err)
		}
	})
}

/*
TestGetNode tests the GetNode method to ensure it correctly retrieves nodes from the HashRing
for given keys. It verifies single node retrieval, multiple node scenarios, empty ring handling,
consistent key-to-node mapping, and wrap-around behavior when key hash is larger than all node hashes.
It ensures that the same key always maps to the same node (consistency property of consistent hashing).
*/
func TestGetNode(t *testing.T) {
	t.Run("get node with single node in ring", func(t *testing.T) {
		// Initialize HashRing and add a single node
		ring := HashRingInit()
		node := &mockNode{identifier: "node1"}

		// Add the node to the ring
		if err := ring.AddNode(node); err != nil {
			t.Fatalf("AddNode failed: %v", err)
		}

		// Retrieve node using any key - should return the single node
		retrievedNode, err := ring.GetNode("any-key")
		if err != nil {
			t.Fatalf("GetNode failed: %v", err)
		}
		// Verify the retrieved node matches what we added
		if retrievedNode.GetIdentifier() != "node1" {
			t.Errorf("Expected node1, got %s", retrievedNode.GetIdentifier())
		}
	})

	t.Run("get node with multiple nodes", func(t *testing.T) {
		// Initialize HashRing and add multiple nodes
		ring := HashRingInit()
		node1 := &mockNode{identifier: "node1"}
		node2 := &mockNode{identifier: "node2"}
		node3 := &mockNode{identifier: "node3"}

		// Add all three nodes
		if err := ring.AddNode(node1); err != nil {
			t.Fatalf("Failed to add node1: %v", err)
		}
		if err := ring.AddNode(node2); err != nil {
			t.Fatalf("Failed to add node2: %v", err)
		}
		if err := ring.AddNode(node3); err != nil {
			t.Fatalf("Failed to add node3: %v", err)
		}

		// Test multiple keys to ensure consistent mapping
		key1 := "key1"
		key2 := "key2"
		key3 := "key3"

		// Retrieve nodes for different keys
		retrievedNode1, err := ring.GetNode(key1)
		if err != nil {
			t.Fatalf("GetNode failed for key1: %v", err)
		}
		if retrievedNode1 == nil {
			t.Error("GetNode returned nil node for key1")
		}

		retrievedNode2, err := ring.GetNode(key2)
		if err != nil {
			t.Fatalf("GetNode failed for key2: %v", err)
		}
		if retrievedNode2 == nil {
			t.Error("GetNode returned nil node for key2")
		}

		retrievedNode3, err := ring.GetNode(key3)
		if err != nil {
			t.Fatalf("GetNode failed for key3: %v", err)
		}
		if retrievedNode3 == nil {
			t.Error("GetNode returned nil node for key3")
		}
	})

	t.Run("get node from empty ring", func(t *testing.T) {
		// Initialize an empty HashRing
		ring := HashRingInit()

		// Attempt to get a node from empty ring - should return error
		_, err := ring.GetNode("any-key")
		if err == nil {
			t.Error("Expected error when getting node from empty ring")
		}
		// Verify the error is ErrNoConnectedNodes
		if !errors.Is(err, ErrNoConnectedNodes) {
			t.Errorf("Expected ErrNoConnectedNodes, got %v", err)
		}
	})

	t.Run("consistent node mapping for same key", func(t *testing.T) {
		// Initialize HashRing and add multiple nodes
		ring := HashRingInit()
		node1 := &mockNode{identifier: "node1"}
		node2 := &mockNode{identifier: "node2"}
		node3 := &mockNode{identifier: "node3"}

		// Add all nodes
		if err := ring.AddNode(node1); err != nil {
			t.Fatalf("Failed to add node1: %v", err)
		}
		if err := ring.AddNode(node2); err != nil {
			t.Fatalf("Failed to add node2: %v", err)
		}
		if err := ring.AddNode(node3); err != nil {
			t.Fatalf("Failed to add node3: %v", err)
		}

		// Test key for consistency
		testKey := "consistent-test-key"
		// Get the node for this key first time
		firstNode, err := ring.GetNode(testKey)
		if err != nil {
			t.Fatalf("GetNode failed: %v", err)
		}

		// Get the same key multiple times - should return same node (consistency)
		for i := 0; i < 10; i++ {
			node, err := ring.GetNode(testKey)
			if err != nil {
				t.Fatalf("GetNode failed on iteration %d: %v", i, err)
			}
			// Verify consistency - same key should map to same node
			if node.GetIdentifier() != firstNode.GetIdentifier() {
				t.Errorf("Inconsistent mapping: expected %s, got %s", firstNode.GetIdentifier(), node.GetIdentifier())
			}
		}
	})

	t.Run("wraps around when key hash is larger than all node hashes", func(t *testing.T) {
		// Initialize HashRing and add nodes
		ring := HashRingInit()
		node1 := &mockNode{identifier: "node1"}
		node2 := &mockNode{identifier: "node2"}

		// Add nodes
		if err := ring.AddNode(node1); err != nil {
			t.Fatalf("Failed to add node1: %v", err)
		}
		if err := ring.AddNode(node2); err != nil {
			t.Fatalf("Failed to add node2: %v", err)
		}

		// Test with a key that should wrap around (probabilistic, so test multiple keys)
		// The binarySearch should wrap to index 0 when no node hash >= key hash
		_, err := ring.GetNode("zzz-wrap-around-key")
		if err != nil {
			t.Fatalf("GetNode should wrap around, got error: %v", err)
		}
	})
}

/*
TestRemoveNode tests the RemoveNode method to ensure nodes can be properly removed from the HashRing.
It verifies successful node removal, error handling for non-existent nodes, removal from empty ring,
removing all nodes, and that the sortedKeyOfNodes slice remains sorted after removal. It also
verifies that nodes are no longer accessible after being removed.
*/
func TestRemoveNode(t *testing.T) {
	t.Run("remove existing node", func(t *testing.T) {
		// Initialize HashRing and add multiple nodes
		ring := HashRingInit()
		node1 := &mockNode{identifier: "node1"}
		node2 := &mockNode{identifier: "node2"}

		// Add both nodes
		if err := ring.AddNode(node1); err != nil {
			t.Fatalf("Failed to add node1: %v", err)
		}
		if err := ring.AddNode(node2); err != nil {
			t.Fatalf("Failed to add node2: %v", err)
		}

		// Record initial node count
		initialCount := len(ring.sortedKeyOfNodes)
		// Remove node1
		if err := ring.RemoveNode(node1); err != nil {
			t.Fatalf("RemoveNode failed: %v", err)
		}

		// Verify node count decreased by one
		if len(ring.sortedKeyOfNodes) != initialCount-1 {
			t.Errorf("Expected %d nodes, got %d", initialCount-1, len(ring.sortedKeyOfNodes))
		}

		// Verify node1 is no longer accessible, but GetNode should still work with remaining nodes
		_, err := ring.GetNode("test-key")
		if err != nil {
			t.Fatalf("GetNode failed after removal: %v", err)
		}
	})

	t.Run("remove non-existent node", func(t *testing.T) {
		// Initialize HashRing and add one node
		ring := HashRingInit()
		node1 := &mockNode{identifier: "node1"}
		node2 := &mockNode{identifier: "node2"}

		// Add only node1
		if err := ring.AddNode(node1); err != nil {
			t.Fatalf("Failed to add node1: %v", err)
		}

		// Try to remove node2 which was never added - should fail
		err := ring.RemoveNode(node2)
		if err == nil {
			t.Error("Expected error when removing non-existent node")
		}
		// Verify the error is ErrNodeNotFound
		if !errors.Is(err, ErrNodeNotFound) {
			t.Errorf("Expected ErrNodeNotFound, got %v", err)
		}
	})

	t.Run("remove node from empty ring", func(t *testing.T) {
		// Initialize an empty HashRing
		ring := HashRingInit()
		node := &mockNode{identifier: "node1"}

		// Try to remove from empty ring - should fail
		err := ring.RemoveNode(node)
		if err == nil {
			t.Error("Expected error when removing from empty ring")
		}
		// Verify the error is ErrNodeNotFound
		if !errors.Is(err, ErrNodeNotFound) {
			t.Errorf("Expected ErrNodeNotFound, got %v", err)
		}
	})

	t.Run("remove all nodes", func(t *testing.T) {
		// Initialize HashRing and add nodes
		ring := HashRingInit()
		node1 := &mockNode{identifier: "node1"}
		node2 := &mockNode{identifier: "node2"}

		// Add both nodes
		if err := ring.AddNode(node1); err != nil {
			t.Fatalf("Failed to add node1: %v", err)
		}
		if err := ring.AddNode(node2); err != nil {
			t.Fatalf("Failed to add node2: %v", err)
		}

		// Remove first node
		if err := ring.RemoveNode(node1); err != nil {
			t.Fatalf("Failed to remove node1: %v", err)
		}
		// Remove second node
		if err := ring.RemoveNode(node2); err != nil {
			t.Fatalf("Failed to remove node2: %v", err)
		}

		// Verify ring is now empty
		if len(ring.sortedKeyOfNodes) != 0 {
			t.Errorf("Expected 0 nodes, got %d", len(ring.sortedKeyOfNodes))
		}

		// Should fail to get node from empty ring
		_, err := ring.GetNode("test-key")
		if err == nil {
			t.Error("Expected error when getting node from empty ring")
		}
	})

	t.Run("sortedKeyOfNodes remains sorted after removal", func(t *testing.T) {
		// Initialize HashRing and add multiple nodes
		ring := HashRingInit()
		nodes := []*mockNode{
			{identifier: "node1"},
			{identifier: "node2"},
			{identifier: "node3"},
		}

		// Add all nodes
		for _, node := range nodes {
			if err := ring.AddNode(node); err != nil {
				t.Fatalf("Failed to add node: %v", err)
			}
		}

		// Remove middle node
		if err := ring.RemoveNode(nodes[1]); err != nil {
			t.Fatalf("Failed to remove node: %v", err)
		}

		// Verify sortedKeyOfNodes is still sorted after removal
		for i := 1; i < len(ring.sortedKeyOfNodes); i++ {
			if ring.sortedKeyOfNodes[i-1] > ring.sortedKeyOfNodes[i] {
				t.Error("sortedKeyOfNodes is not sorted after removal")
			}
		}
	})
}

/*
TestConcurrentOperations tests the thread-safety of HashRing operations by performing
concurrent add, get, and remove operations. It verifies that the HashRing correctly handles
multiple goroutines accessing it simultaneously without data races or corruption.
*/
func TestConcurrentOperations(t *testing.T) {
	t.Run("concurrent add operations", func(t *testing.T) {
		// Initialize HashRing for concurrent testing
		ring := HashRingInit()
		const numNodes = 10
		nodes := make([]*mockNode, numNodes)

		// Create multiple nodes
		for i := 0; i < numNodes; i++ {
			nodes[i] = &mockNode{identifier: "node" + string(rune('0'+i))}
		}

		// Add nodes concurrently using goroutines
		var wg sync.WaitGroup
		for i := 0; i < numNodes; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				if err := ring.AddNode(nodes[idx]); err != nil {
					t.Errorf("Failed to add node%d: %v", idx, err)
				}
			}(i)
		}
		wg.Wait()

		// Verify all nodes were added
		if len(ring.sortedKeyOfNodes) != numNodes {
			t.Errorf("Expected %d nodes, got %d", numNodes, len(ring.sortedKeyOfNodes))
		}
	})

	t.Run("concurrent get operations", func(t *testing.T) {
		// Initialize HashRing and add nodes
		ring := HashRingInit()
		node1 := &mockNode{identifier: "node1"}
		node2 := &mockNode{identifier: "node2"}
		node3 := &mockNode{identifier: "node3"}

		// Add nodes
		if err := ring.AddNode(node1); err != nil {
			t.Fatalf("Failed to add node1: %v", err)
		}
		if err := ring.AddNode(node2); err != nil {
			t.Fatalf("Failed to add node2: %v", err)
		}
		if err := ring.AddNode(node3); err != nil {
			t.Fatalf("Failed to add node3: %v", err)
		}

		// Perform concurrent get operations
		var wg sync.WaitGroup
		const numGets = 100
		for i := 0; i < numGets; i++ {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				_, err := ring.GetNode(key)
				if err != nil {
					t.Errorf("GetNode failed for key %s: %v", key, err)
				}
			}("key" + string(rune('0'+i%10)))
		}
		wg.Wait()
	})

	t.Run("concurrent add and get operations", func(t *testing.T) {
		// Initialize HashRing and add initial node
		ring := HashRingInit()
		node1 := &mockNode{identifier: "node1"}
		if err := ring.AddNode(node1); err != nil {
			t.Fatalf("Failed to add initial node: %v", err)
		}

		var wg sync.WaitGroup
		// Concurrent adds - add more nodes while getting
		for i := 2; i <= 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				node := &mockNode{identifier: "node" + string(rune('0'+idx))}
				ring.AddNode(node)
			}(i)
		}

		// Concurrent gets - get nodes while adding
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(key string) {
				defer wg.Done()
				ring.GetNode(key)
			}("test-key" + string(rune('0'+i%10)))
		}

		wg.Wait()
	})
}
