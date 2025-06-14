package web

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"log"
	"sort"
)

type StorageNode struct {
	Address  string // server address in the format "host:port"
	NodeHash uint64 // corresponding hash values
}

type ConsistentHashRing struct {
	Nodes []StorageNode
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
	}

	// iterate over servers
	for i, serverAddr := range servers {
		chRing.Nodes[i] = StorageNode{
			Address:  serverAddr,
			NodeHash: hashStringToUint64(serverAddr),
		}

	}

	chRing.SortRing()

	return chRing, nil
}

func (ring *ConsistentHashRing) GetNodesInRing() []string {
	// Extract address from ring.Nodes and return in a slice
	nodes := make([]string, len(ring.Nodes))
	for i, node := range ring.Nodes {
		nodes[i] = node.Address
	}
	return nodes
}

func (ring *ConsistentHashRing) GetNumNodes() int {
	return len(ring.Nodes)
}

// Sort the nodes by their hash values
func (ring *ConsistentHashRing) SortRing() {
	sort.Slice(ring.Nodes, func(i, j int) bool {
		return ring.Nodes[i].NodeHash < ring.Nodes[j].NodeHash
	})
}

// Get the node that should store a given key
func (ring *ConsistentHashRing) GetNodeFromKey(key string) string {
	if len(ring.Nodes) == 0 {
		log.Printf("len(ring.Nodes) is 0, ring.Nodes is %v", ring.Nodes)
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
	idx = idx % len(ring.Nodes)

	return ring.Nodes[idx].Address
}

// ONLY USE THIS if it is certain that `nodeAddr` exists in the ring
func (ring *ConsistentHashRing) GetSuccessorNode(nodeAddr string) string {
	if len(ring.Nodes) == 0 {
		log.Printf("len(ring.Nodes) is 0, ring.Nodes is %v", ring.Nodes)
		return ""
	}

	if len(ring.Nodes) == 1 {
		if ring.Nodes[0].Address == nodeAddr {
			return ring.Nodes[0].Address
		} else {
			return ""
		}
	}

	nodeHash := hashStringToUint64(nodeAddr)
	idx := sort.Search(len(ring.Nodes), func(i int) bool {
		return ring.Nodes[i].NodeHash == nodeHash
	})

	idx = (idx + 1) % (len(ring.Nodes))

	return ring.Nodes[idx].Address
}

func (ring *ConsistentHashRing) AddNodeToRing(node string) {
	newNode := StorageNode{
		Address:  node,
		NodeHash: hashStringToUint64(node),
	}

	ring.Nodes = append(ring.Nodes, newNode)
	ring.SortRing()
}

func (ring *ConsistentHashRing) DeleteNodeFromRing(node string) bool {

	found := false // indicate if the node to delete was found
	idx := -1      // index of node to delete

	for i, storageNode := range ring.Nodes {
		if node == storageNode.Address {
			idx = i
			found = true
			break
		}
	}
	// remove element at index `idx`
	if found && idx != -1 {
		ring.Nodes = append(ring.Nodes[:idx], ring.Nodes[idx+1:]...)
	}
	return found
}

// Returns the node that would be responsible for the key
// in the old ring configuration (used for migration)
func (r *ConsistentHashRing) GetNodeForKeyWithNodes(key string, nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}

	if len(nodes) == 1 {
		return nodes[0]
	}

	// Create temporary ring with old nodes
	tempRing, _ := NewConsistentHashRing(nodes)
	return tempRing.GetNodeFromKey(key)
}
