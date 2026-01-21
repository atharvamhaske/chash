package main

import (
	"fmt"
	"log"
	"strings"

	hashring "github.com/atharvamhaske/chash/hash-ring"
)

// DemoNode implements the CacheNode interface for demonstration
type DemoNode struct {
	ID   string
	Data map[string]string
}

func (n *DemoNode) GetIdentifier() string {
	return n.ID
}

func main() {
	fmt.Println("=== Consistent Hashing Demo ===")
	fmt.Println()

	// Initialize hash ring with verbose logging enabled
	ring := hashring.HashRingInit(hashring.EnableVerboseLogs(true))
	fmt.Println("Initialized hash ring")
	fmt.Println()

	// Create demo nodes (representing database shards or cache servers)
	node1 := &DemoNode{ID: "node-1", Data: make(map[string]string)}
	node2 := &DemoNode{ID: "node-2", Data: make(map[string]string)}
	node3 := &DemoNode{ID: "node-3", Data: make(map[string]string)}

	// Add nodes to the hash ring
	fmt.Println("Adding nodes to hash ring...")
	if err := ring.AddNode(node1); err != nil {
		log.Fatalf("Failed to add node1: %v", err)
	}
	fmt.Println("✓ Added node-1")

	if err := ring.AddNode(node2); err != nil {
		log.Fatalf("Failed to add node2: %v", err)
	}
	fmt.Println("✓ Added node-2")

	if err := ring.AddNode(node3); err != nil {
		log.Fatalf("Failed to add node3: %v", err)
	}
	fmt.Println("✓ Added node-3")
	fmt.Println()

	// Simulate storing keys and finding which node handles them
	testKeys := []string{
		"user:123",
		"user:456",
		"product:789",
		"order:101",
		"cart:202",
		"session:303",
	}

	fmt.Println("Mapping keys to nodes:")
	fmt.Println("----------------------")
	for _, key := range testKeys {
		node, err := ring.GetNode(key)
		if err != nil {
			log.Printf("Error getting node for key %s: %v", key, err)
			continue
		}
		fmt.Printf("Key: %-15s -> Node: %s\n", key, node.GetIdentifier())
	}

	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("Demonstrating node removal...")
	fmt.Println(strings.Repeat("=", 50) + "\n")

	// Remove a node and show how keys are redistributed
	fmt.Println("Removing node-2...")
	if err := ring.RemoveNode(node2); err != nil {
		log.Fatalf("Failed to remove node2: %v", err)
	}
	fmt.Println("✓ Removed node-2")
	fmt.Println()

	fmt.Println("Key mapping after node removal:")
	fmt.Println("-------------------------------")
	for _, key := range testKeys {
		node, err := ring.GetNode(key)
		if err != nil {
			log.Printf("Error getting node for key %s: %v", key, err)
			continue
		}
		fmt.Printf("Key: %-15s -> Node: %s\n", key, node.GetIdentifier())
	}

	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("Demonstrating duplicate node prevention...")
	fmt.Println(strings.Repeat("=", 50) + "\n")

	// Try to add a node with the same identifier
	duplicateNode := &DemoNode{ID: "node-1", Data: make(map[string]string)}
	if err := ring.AddNode(duplicateNode); err != nil {
		fmt.Printf("✓ Correctly prevented duplicate node: %v\n", err)
	}

	fmt.Println("\n" + strings.Repeat("=", 50))
	fmt.Println("Demo completed successfully!")
	fmt.Println(strings.Repeat("=", 50))
}
