// Lab 8: Implement a network video content service (client using consistent hashing)

package web

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
	pb "tritontube/internal/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NetworkVideoContentService implements VideoContentService using a network of nodes.
type NetworkVideoContentService struct {
	Ring        *ConsistentHashRing
	Clients     map[string]pb.StorageServiceClient // gRPC clients
	Conns       map[string]*grpc.ClientConn        // Client connection pool
	AdminServer *grpc.Server                       // for admin operations
	AdminAddr   string                             // Address of admin server
	Mu          sync.RWMutex
}

// Uncomment the following line to ensure NetworkVideoContentService implements VideoContentService
// var _ VideoContentService = (*NetworkVideoContentService)(nil)

func NewNetworkVideoContentService(nodes string) (*NetworkVideoContentService, error) {
	// Parse string into list
	grpcNodes := strings.Split(nodes, ",")
	if len(grpcNodes) == 0 {
		return nil, errors.New("failed to parse comma-separated nodes list")
	}

	adminServerAddr := grpcNodes[0]
	storageNodeAddrs := grpcNodes[1:]

	ring, err := NewConsistentHashRing(storageNodeAddrs)
	if err != nil {
		return nil, err
	}

	service := &NetworkVideoContentService{
		Ring:      ring,
		Clients:   make(map[string]pb.StorageServiceClient),
		Conns:     make(map[string]*grpc.ClientConn),
		AdminAddr: adminServerAddr,
		// AdminServer: ,
	}

	// Create gRPC clients and connections for all storage servers
	for _, node := range grpcNodes {
		if err := service.createClientForNode(node); err != nil {
			log.Printf("Warning: Failed to create client for node %s: %v", node, err)
			// Continue with other nodes, but log the failure
		}
	}

	// Start the admin gRPC server
	// if err := service.startAdminServer(); err != nil {
	// 	return nil, fmt.Errorf("failed to start admin server: %w", err)
	// }

	return service, nil
}

func (nw *NetworkVideoContentService) createClientForNode(node string) error {
	conn, err := grpc.NewClient(node, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return fmt.Errorf("failed to connect to storage node %s: %w", node, err)
	}

	client := pb.NewStorageServiceClient(conn)

	nw.Mu.Lock()
	nw.Conns[node] = conn
	nw.Clients[node] = client
	nw.Mu.Unlock()
	return nil
}

func (nw *NetworkVideoContentService) getClientForNode(node string) (pb.StorageServiceClient, error) {
	nw.Mu.RLock()
	client, exists := nw.Clients[node]
	nw.Mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no client available for node %s", node)
	}

	return client, nil
}

// func (nw *NetworkVideoContentService) deleteClientForNode(node string) {
// 	nw.Mu.Lock()
// 	defer nw.Mu.Unlock()

// 	if conn, exists := nw.Conns[node]; exists {
// 		conn.Close()
// 		delete(nw.Conns, node)
// 	}
// 	delete(nw.Clients, node)
// }

// // startAdminServer starts the gRPC server for admin operations
// func (nw *NetworkVideoContentService) startAdminServer() error {
// 	lis, err := net.Listen("tcp", nw.AdminAddr)
// 	if err != nil {
// 		return fmt.Errorf("failed to listen on %s: %w", nw.AdminAddr, err)
// 	}

// 	nw.AdminServer = grpc.NewServer()

// 	// Register the admin service (we'll implement this interface next)
// 	proto.RegisterVideoContentAdminServiceServer(nw.AdminServer, nw)

// 	// Start serving in a goroutine
// 	go func() {
// 		log.Printf("Admin gRPC server starting on %s", nw.AdminAddr)
// 		if err := nw.AdminServer.Serve(lis); err != nil {
// 			log.Printf("Admin server error: %v", err)
// 		}
// 	}()

// 	return nil
// }

// Close cleans up all connections and stops the admin server
func (nw *NetworkVideoContentService) Close() error {
	// Stop admin server
	if nw.AdminServer != nil {
		nw.AdminServer.GracefulStop()
	}

	// Close all storage server connections
	nw.Mu.Lock()
	defer nw.Mu.Unlock()

	for nodeAddr, conn := range nw.Conns {
		if err := conn.Close(); err != nil {
			log.Printf("Error closing connection to %s: %v", nodeAddr, err)
		}
	}

	// Clear the maps
	nw.Clients = make(map[string]pb.StorageServiceClient)
	nw.Conns = make(map[string]*grpc.ClientConn)

	return nil
}

func (nw *NetworkVideoContentService) Read(videoId string, filename string) ([]byte, error) {
	hashKey := fmt.Sprintf("%s/%s", videoId, filename)

	targetNode := nw.Ring.GetNodeFromKey(hashKey)
	if targetNode == "" {
		return nil, fmt.Errorf("failed to get storage node for key %v", hashKey)
	}

	client, err := nw.getClientForNode(targetNode)
	if err != nil {
		return nil, fmt.Errorf("failed to get grpc Client for storage node %v", targetNode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.GetFile(ctx, &pb.GetFileRequest{
		VideoId:  videoId,
		Filename: filename,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve file from node %s: %w", targetNode, err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("storage server error: %s", resp.Error)
	}

	return resp.Content, nil
}

func (nw *NetworkVideoContentService) Write(videoId string, filename string, data []byte) error {
	hashKey := fmt.Sprintf("%s/%s", videoId, filename)

	targetNode := nw.Ring.GetNodeFromKey(hashKey)
	if targetNode == "" {
		return fmt.Errorf("failed to get storage node for key %v", hashKey)
	}

	client, err := nw.getClientForNode(targetNode)
	if err != nil {
		return fmt.Errorf("failed to get grpc Client for storage node %v", targetNode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.AddFile(ctx, &pb.AddFileRequest{
		VideoId:  videoId,
		Filename: filename,
		Content:  data,
	})

	if err != nil {
		return fmt.Errorf("failed to write file to node %s: %w", targetNode, err)
	}
	if !resp.Success {
		return fmt.Errorf("storage server error: %s", resp.Error)
	}
	return nil
}
