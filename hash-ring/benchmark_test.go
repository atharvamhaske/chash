package hashring

import (
	"hash/fnv"
	"testing"
)

type benchmarkNode struct {
	identifier string
}

func (b *benchmarkNode) GetIdentifier() string {
	return b.identifier
}

func BenchmarkAddNode(b *testing.B) {
	ring := HashRingInit()
	nodes := make([]*benchmarkNode, b.N)
	for i := 0; i < b.N; i++ {
		nodes[i] = &benchmarkNode{identifier: "node" + string(rune(i))}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.AddNode(nodes[i])
	}
}

func BenchmarkGetNode(b *testing.B) {
	ring := HashRingInit()
	// Add 100 nodes
	for i := 0; i < 100; i++ {
		node := &benchmarkNode{identifier: "node" + string(rune(i))}
		ring.AddNode(node)
	}

	testKeys := []string{
		"user:123", "user:456", "product:789", "order:101",
		"cart:202", "session:303", "item:404", "data:505",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := testKeys[i%len(testKeys)]
		ring.GetNode(key)
	}
}

func BenchmarkRemoveNode(b *testing.B) {
	ring := HashRingInit()
	nodes := make([]*benchmarkNode, b.N+10)
	for i := 0; i < b.N+10; i++ {
		nodes[i] = &benchmarkNode{identifier: "node" + string(rune(i))}
		ring.AddNode(nodes[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ring.RemoveNode(nodes[i])
	}
}

func BenchmarkConcurrentGetNode(b *testing.B) {
	ring := HashRingInit()
	// Add 50 nodes
	for i := 0; i < 50; i++ {
		node := &benchmarkNode{identifier: "node" + string(rune(i))}
		ring.AddNode(node)
	}

	testKeys := []string{
		"key1", "key2", "key3", "key4", "key5",
		"key6", "key7", "key8", "key9", "key10",
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := testKeys[i%len(testKeys)]
			ring.GetNode(key)
			i++
		}
	})
}

func BenchmarkBinarySearch(b *testing.B) {
	ring := HashRingInit()
	// Add 1000 nodes for a larger search space
	for i := 0; i < 1000; i++ {
		node := &benchmarkNode{identifier: "node" + string(rune(i))}
		ring.AddNode(node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		testHash := int64(i % 10000)
		ring.binarySearch(testHash)
	}
}

func BenchmarkHashRingWithCustomHash(b *testing.B) {
	ring := HashRingInit(SetHashFunction(fnv.New64))
	for i := 0; i < 100; i++ {
		node := &benchmarkNode{identifier: "node" + string(rune(i))}
		ring.AddNode(node)
	}

	testKeys := []string{"key1", "key2", "key3", "key4", "key5"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := testKeys[i%len(testKeys)]
		ring.GetNode(key)
	}
}
