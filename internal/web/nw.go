// Lab 8: Implement a network video content service (client using consistent hashing)

package web

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
	pb "tritontube/internal/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NetworkVideoContentService implements VideoContentService using a network of nodes.
type NetworkVideoContentService struct {
	pb.UnimplementedVideoContentAdminServiceServer
	Ring        *ConsistentHashRing
	Clients     map[string]pb.StorageServiceClient // gRPC clients
	Conns       map[string]*grpc.ClientConn        // Client connection pool
	AdminServer *grpc.Server                       // for admin operations
	AdminAddr   string                             // Address of admin server
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
	for _, node := range storageNodeAddrs {
		if err := service.createClientForNode(node); err != nil {
			log.Printf("Warning: Failed to create client for node %s: %v", node, err)
			// Continue with other nodes, but log the failure
		}
	}

	// Start the admin gRPC server
	if err := service.startAdminServer(); err != nil {
		return nil, fmt.Errorf("failed to start admin server: %w", err)
	}

	return service, nil
}

func (nw *NetworkVideoContentService) createClientForNode(node string) error {
	conn, err := grpc.NewClient(node, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		return fmt.Errorf("failed to connect to storage node %s: %w", node, err)
	}

	client := pb.NewStorageServiceClient(conn)

	nw.Conns[node] = conn
	nw.Clients[node] = client
	return nil
}

func (nw *NetworkVideoContentService) getClientForNode(node string) (pb.StorageServiceClient, error) {
	client, exists := nw.Clients[node]

	if !exists {
		return nil, fmt.Errorf("no client available for node %s", node)
	}

	return client, nil
}

func (nw *NetworkVideoContentService) deleteClientForNode(node string) {

	if conn, exists := nw.Conns[node]; exists {
		conn.Close()
		delete(nw.Conns, node)
	}
	delete(nw.Clients, node)
}

func (nw *NetworkVideoContentService) ListNodes(ctx context.Context, req *pb.ListNodesRequest) (*pb.ListNodesResponse, error) {
	nodes := nw.Ring.GetNodesInRing()

	return &pb.ListNodesResponse{
		Nodes: nodes,
	}, nil
}

// Add a new node to the ring and split the files of the successor node between it and the newly added node
func (nw *NetworkVideoContentService) AddNode(ctx context.Context, req *pb.AddNodeRequest) (*pb.AddNodeResponse, error) {

	newNodeAddr := req.NodeAddress

	// Check if node already exists
	currentNodes := nw.Ring.GetNodesInRing()
	for _, node := range currentNodes {
		if node == newNodeAddr {
			return &pb.AddNodeResponse{
				MigratedFileCount: 0,
			}, fmt.Errorf("node %v already exists in cluster", newNodeAddr)
		}
	}

	log.Printf("creating client for node %v", newNodeAddr)

	// Create grpc client for node
	err := nw.createClientForNode(newNodeAddr)
	if err != nil {
		return &pb.AddNodeResponse{
			MigratedFileCount: 0,
		}, fmt.Errorf("failed to create gRPC client for node %v: %v", newNodeAddr, err)
	}

	// Add the node to the ring (this changes the consistent hashing)
	nw.Ring.AddNodeToRing(newNodeAddr)

	log.Printf("initiating migration process")

	// Get the successor of the current node
	succNode := nw.Ring.GetSuccessorNode(newNodeAddr)

	// get gRPC client for the successor node
	client, err := nw.getClientForNode(succNode)
	if err != nil {
		return &pb.AddNodeResponse{
			MigratedFileCount: 0,
		}, fmt.Errorf("failed to get gRPC client for node %v: %v", succNode, err)
	}

	// Get all files on successor node
	files, err := client.ListAllFiles(ctx, &pb.ListAllFilesRequest{})
	if err != nil {
		return &pb.AddNodeResponse{
			MigratedFileCount: 0,
		}, fmt.Errorf("failed to list files on %v: %v", succNode, err)
	}

	// Select files to send to new node
	filesToMigrate := make([]string, 0)

	for _, f := range files.Files {
		fileKey := fmt.Sprintf("%s/%s", f.VideoId, f.Filename)
		if nw.Ring.GetNodeFromKey(fileKey) == newNodeAddr {
			filesToMigrate = append(filesToMigrate, fileKey)
		}
	}

	// Perform the actual file migrations
	migratedCount := 0
	var migrationErrors []string

	for _, fileKey := range filesToMigrate {
		// Parse the file key back to videoId and filename
		parts := strings.SplitN(fileKey, "/", 2)
		if len(parts) != 2 {
			migrationErrors = append(migrationErrors, fmt.Sprintf("invalid file key format: %s", fileKey))
			continue
		}

		videoId := parts[0]
		filename := parts[1]

		// Migrate the file
		if err := nw.moveFile(succNode, newNodeAddr, videoId, filename); err != nil {
			migrationErrors = append(migrationErrors, fmt.Sprintf("failed to migrate %s: %v", fileKey, err))
			continue
		}

		migratedCount++
	}

	// If there were migration errors, log them but still report success if some files migrated
	if len(migrationErrors) > 0 {
		log.Printf("Migration errors when adding node %s: %v", newNodeAddr, migrationErrors)

		// If ALL migrations failed, consider it a failure
		if migratedCount == 0 && len(filesToMigrate) > 0 {
			return &pb.AddNodeResponse{
				MigratedFileCount: 0,
			}, fmt.Errorf("all migrations failed: %v", migrationErrors[0])
		}
	}

	return &pb.AddNodeResponse{
		MigratedFileCount: int32(migratedCount),
	}, nil
}

// Remove the node at the specified address and transfer all of its files to its successor in the ring
func (nw *NetworkVideoContentService) RemoveNode(ctx context.Context, req *pb.RemoveNodeRequest) (*pb.RemoveNodeResponse, error) {
	nodeToRemove := req.NodeAddress

	currNodes := nw.Ring.GetNodesInRing()
	nodeExists := false
	for _, storageNode := range currNodes {
		if storageNode == nodeToRemove {
			nodeExists = true
			break
		}
	}
	if !nodeExists {
		return &pb.RemoveNodeResponse{
			MigratedFileCount: 0,
		}, fmt.Errorf("node %v does not exist in ring, cannot delete", nodeToRemove)
	}

	if len(currNodes) <= 1 {
		return &pb.RemoveNodeResponse{
			MigratedFileCount: 0,
		}, errors.New("there are too few nodes in the ring, cannot remove")
	}

	// Get the successor of the node to remove
	succNode := nw.Ring.GetSuccessorNode(nodeToRemove)

	// get gRPC client for the node to remove
	client, err := nw.getClientForNode(nodeToRemove)
	if err != nil {
		return &pb.RemoveNodeResponse{
			MigratedFileCount: 0,
		}, fmt.Errorf("failed to get gRPC client for node %v: %v", nodeToRemove, err)
	}

	// Get all files on node to remove
	files, err := client.ListAllFiles(ctx, &pb.ListAllFilesRequest{})
	if err != nil {
		return &pb.RemoveNodeResponse{
			MigratedFileCount: 0,
		}, fmt.Errorf("failed to list files on %v: %v", succNode, err)
	}

	filesToMigrate := make([]string, 0)

	for _, f := range files.Files {
		fileKey := fmt.Sprintf("%s/%s", f.VideoId, f.Filename)
		filesToMigrate = append(filesToMigrate, fileKey)
	}

	// Perform the actual file migrations
	migratedCount := 0
	var migrationErrors []string

	if !nw.Ring.DeleteNodeFromRing(nodeToRemove) {
		return &pb.RemoveNodeResponse{
			MigratedFileCount: 0,
		}, fmt.Errorf("failed to remove node %v from ring", nodeToRemove)
	}

	for _, fileKey := range filesToMigrate {
		// Parse the file key back to videoId and filename
		parts := strings.SplitN(fileKey, "/", 2)
		if len(parts) != 2 {
			migrationErrors = append(migrationErrors, fmt.Sprintf("invalid file key format: %s", fileKey))
			continue
		}

		videoId := parts[0]
		filename := parts[1]

		// Migrate the file
		if err := nw.moveFile(nodeToRemove, succNode, videoId, filename); err != nil {
			migrationErrors = append(migrationErrors, fmt.Sprintf("failed to migrate %s: %v", fileKey, err))
			continue
		}

		migratedCount++
	}

	// Clean up the gRPC client connection for the removed node
	nw.deleteClientForNode(nodeToRemove)

	// Log migration errors but still report success if some files migrated
	if len(migrationErrors) > 0 {
		log.Printf("Migration errors when removing node %s: %v", nodeToRemove, migrationErrors)

		// If ALL migrations failed, it's a serious problem
		if migratedCount == 0 && len(filesToMigrate) > 0 {
			return &pb.RemoveNodeResponse{
				MigratedFileCount: 0,
			}, fmt.Errorf("all migrations failed: %v", migrationErrors[0])
		}
	}

	return &pb.RemoveNodeResponse{
		MigratedFileCount: int32(migratedCount),
	}, nil
}

// startAdminServer starts the gRPC server for admin operations
func (nw *NetworkVideoContentService) startAdminServer() error {
	lis, err := net.Listen("tcp", nw.AdminAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", nw.AdminAddr, err)
	}

	nw.AdminServer = grpc.NewServer()

	// Register the admin service (we'll implement this interface next)
	pb.RegisterVideoContentAdminServiceServer(nw.AdminServer, nw)

	// Start serving in a goroutine
	go func() {
		log.Printf("Admin gRPC server starting on %s", nw.AdminAddr)
		if err := nw.AdminServer.Serve(lis); err != nil {
			log.Printf("Admin server error: %v", err)
		}
	}()

	return nil
}

// Close cleans up all connections and stops the admin server
func (nw *NetworkVideoContentService) Close() error {
	// Stop admin server
	if nw.AdminServer != nil {
		nw.AdminServer.GracefulStop()
	}

	// Close all storage server connections

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
	fileKey := fmt.Sprintf("%s/%s", videoId, filename)

	targetNode := nw.Ring.GetNodeFromKey(fileKey)
	if targetNode == "" {
		return nil, fmt.Errorf("failed to get storage node for key %v", fileKey)
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
	fileKey := fmt.Sprintf("%s/%s", videoId, filename)

	targetNode := nw.Ring.GetNodeFromKey(fileKey)
	if targetNode == "" {
		return fmt.Errorf("failed to get storage node for key %v", fileKey)
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

// Helper method to migrate a file from one node to another
func (nw *NetworkVideoContentService) moveFile(sourceNode, targetNode, videoId, filename string) error {
	// Get clients for both nodes
	log.Printf("source %v, target %v", sourceNode, targetNode)
	sourceClient, err := nw.getClientForNode(sourceNode)
	if err != nil {
		return fmt.Errorf("failed to get source client: %w", err)
	}

	targetClient, err := nw.getClientForNode(targetNode)
	if err != nil {
		return fmt.Errorf("failed to get target client %v: %w", targetNode, err)
	}

	// Read file from source
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	getResponse, err := sourceClient.GetFile(ctx, &pb.GetFileRequest{
		VideoId:  videoId,
		Filename: filename,
	})
	if err != nil {
		return fmt.Errorf("failed to read file from source: %w", err)
	}

	if !getResponse.Success {
		return fmt.Errorf("source read error: %s", getResponse.Error)
	}

	// Write file to target
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()

	storeResponse, err := targetClient.AddFile(ctx2, &pb.AddFileRequest{
		VideoId:  videoId,
		Filename: filename,
		Content:  getResponse.Content,
	})
	if err != nil {
		return fmt.Errorf("failed to write file to target: %w", err)
	}

	if !storeResponse.Success {
		return fmt.Errorf("target write error: %s", storeResponse.Error)
	}

	// Delete file from source
	ctx3, cancel3 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel3()

	deleteResponse, err := sourceClient.DeleteFile(ctx3, &pb.DeleteFileRequest{
		VideoId:  videoId,
		Filename: filename,
	})
	if err != nil {
		return fmt.Errorf("failed to delete file from source: %w", err)
	}

	if !deleteResponse.Success {
		return fmt.Errorf("source delete error: %s", deleteResponse.Error)
	}

	return nil
}
