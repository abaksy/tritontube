package web

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
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

	chRing.SortRing()

	return chRing, nil
}

func (ring *ConsistentHashRing) GetNodesInRing() []string {
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
	if idx == len(ring.Nodes) {
		idx = 0
	}

	return ring.Nodes[idx].Address
}

func (ring *ConsistentHashRing) AddNodeToRing(node string) {
	newNode := StorageNode{
		Address:  node,
		NodeHash: hashStringToUint64(node),
	}

	// ring.Mu.Lock()
	// defer ring.Mu.Unlock()
	ring.Nodes = append(ring.Nodes, newNode)
	ring.SortRing()
}

func (ring *ConsistentHashRing) DeleteNodeFromRing(node string) bool {

	found := false // indicate if the node to delete was found
	idx := -1      // index of node to delete

	// ring.Mu.Lock()
	// defer ring.Mu.Unlock()

	for i, storageNode := range ring.Nodes {
		if node == storageNode.Address {
			idx = i
			found = true
			break
		}
	}
	// remove element at index `idx`
	if idx != -1 {
		ring.Nodes = append(ring.Nodes[:idx], ring.Nodes[idx+1:]...)
	}

	for _, node := range ring.Nodes {
		fmt.Printf("NODE ADDRESS: \"%v\"", node.Address)
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

// GetFilesToMigrate returns files that should move when a node is added
// This helps determine which files need to be migrated to the new node
func (ring *ConsistentHashRing) GetFilesToMigrateOnAdd(newNode string, allFiles []string) []string {
	ring.Mu.RLock()
	oldNodes := make([]string, len(ring.Nodes))
	for _, storageNode := range ring.Nodes {
		oldNodes = append(oldNodes, storageNode.Address)
	}
	ring.Mu.RUnlock()

	// Create new ring with the added node
	newNodes := append(oldNodes, newNode)
	newRing, _ := NewConsistentHashRing(newNodes)

	var filesToMigrate []string
	for _, file := range allFiles {
		oldResponsibleNode := ring.GetNodeForKeyWithNodes(file, oldNodes)
		newResponsibleNode := newRing.GetNodeFromKey(file)

		// If the responsible node changed and it's now the new node, migrate it
		if newResponsibleNode == newNode && oldResponsibleNode != newNode {
			filesToMigrate = append(filesToMigrate, file)
		}
	}

	return filesToMigrate
}

// GetFilesToMigrateOnRemove returns files that need to be moved when a node is removed
func (ring *ConsistentHashRing) GetFilesToMigrateOnRemove(nodeToRemove string, allFiles []string) map[string]string {
	ring.Mu.RLock()
	currentNodes := make([]string, len(ring.Nodes))
	for _, storageNode := range ring.Nodes {
		currentNodes = append(currentNodes, storageNode.Address)
	}
	ring.Mu.RUnlock()

	// Create new ring without the removed node
	newNodes := make([]string, len(currentNodes)-1)
	for _, node := range currentNodes {
		if node != nodeToRemove {
			newNodes = append(newNodes, node)
		}
	}

	if len(newNodes) == 0 {
		return make(map[string]string) // No nodes left
	}

	newRing, _ := NewConsistentHashRing(newNodes)

	log.Printf("length of new ring is: %v", len(newRing.Nodes))
	log.Printf("new ring: %v", newNodes)

	// Map from file to new responsible node
	fileMigrations := make(map[string]string)
	for _, file := range allFiles {
		currentResponsibleNode := ring.GetNodeFromKey(file)
		// If the file is currently on the node being removed, find its new home
		if currentResponsibleNode == nodeToRemove {
			newResponsibleNode := newRing.GetNodeFromKey(file)
			fileMigrations[file] = newResponsibleNode
		}
	}

	return fileMigrations
}
