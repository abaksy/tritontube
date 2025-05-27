package web

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"
	"sort"
	"sync"
)

type StorageNode struct {
	Address  string // server address in the format "host:port"
	NodeHash uint64 // corresponding hash values
}

type ConsistentHashRing struct {
	Nodes []StorageNode
	Mu    sync.RWMutex // for thread safety
}

func hashStringToUint64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

func NewConsistentHashRing(servers []string) (*ConsistentHashRing, error) {
	if len(servers) == 0 {
		return &ConsistentHashRing{}, errors.New("`servers` is empty, could not parse")
	}

	numServers := len(servers)
	chRing := &ConsistentHashRing{
		Nodes: make([]StorageNode, numServers),
		Mu:    sync.RWMutex{},
	}

	// iterate over servers
	for i, serverAddr := range servers {
		chRing.Nodes[i] = StorageNode{
			Address:  serverAddr,
			NodeHash: hashStringToUint64(serverAddr),
		}

	}

	return chRing, nil
}

func (ring *ConsistentHashRing) GetNodes() []string {
	ring.Mu.RLock()
	defer ring.Mu.RUnlock()

	// Extract address from ring.Nodes and return in a slice
	nodes := make([]string, len(ring.Nodes))
	for i, node := range ring.Nodes {
		nodes[i] = node.Address
	}
	return nodes
}

func (ring *ConsistentHashRing) GetNumNodes() int {
	ring.Mu.RLock()
	defer ring.Mu.RUnlock()

	return len(ring.Nodes)
}

// Sort the nodes by their hash values
func (ring *ConsistentHashRing) SortRing() {
	ring.Mu.Lock()
	defer ring.Mu.Unlock()

	sort.Slice(ring.Nodes, func(i, j int) bool {
		return ring.Nodes[i].NodeHash < ring.Nodes[j].NodeHash
	})
}

// Get the node that should store a given key
func (ring *ConsistentHashRing) GetNodeFromKey(key string) string {
	if len(ring.Nodes) == 0 {
		return ""
	}

	if len(ring.Nodes) == 1 {
		return ring.Nodes[0].Address
	}

	keyHash := hashStringToUint64(key)
	idx := sort.Search(len(ring.Nodes), func(i int) bool {
		return ring.Nodes[i].NodeHash >= keyHash
	})

	// If no node found (keyHash > all node hashes), wrap around to first node
	if idx == len(ring.Nodes) {
		idx = 0
	}

	return ring.Nodes[idx].Address
}

func (ring *ConsistentHashRing) AddNode(node string) {
	newNode := StorageNode{
		Address:  node,
		NodeHash: hashStringToUint64(node),
	}

	ring.Mu.Lock()
	defer ring.Mu.Unlock()
	ring.Nodes = append(ring.Nodes, newNode)
	ring.SortRing()
}

func (ring *ConsistentHashRing) DeleteNode(node string) bool {

	found := false // indicate if the node to delete was found
	idx := -1      // index of node to delete

	ring.Mu.Lock()
	defer ring.Mu.Unlock()

	for i, storageNode := range ring.Nodes {
		if node == storageNode.Address {
			idx = i
			found = true
			break
		}
	}
	// remove element at index `idx`
	if idx != -1 {
		ring.Nodes = slices.Delete(ring.Nodes, idx, idx+1)
	}
	return found
}
