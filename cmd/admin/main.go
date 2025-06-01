package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"
	"tritontube/internal/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 3 { // Minimum 3 args: program, command, server_address
		printUsageAndExit()
	}

	cmd := os.Args[1]
	serverAddr := os.Args[2]

	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := proto.NewVideoContentAdminServiceClient(conn)

	switch cmd {
	case "add":
		if len(os.Args) != 4 {
			fmt.Println("Usage: add <server_address> <node_address>")
			os.Exit(1)
		}
		addNode(client, os.Args[3])
	case "remove":
		if len(os.Args) != 4 {
			fmt.Println("Usage: remove <server_address> <node_address>")
			os.Exit(1)
		}
		removeNode(client, os.Args[3])
	case "list":
		if len(os.Args) != 3 {
			fmt.Println("Usage: list <server_address>")
			os.Exit(1)
		}
		listNodes(client)
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		printUsageAndExit()
	}
}

func printUsageAndExit() {
	fmt.Println("Usage:")
	fmt.Println("  add <server_address> <node_address>     - Add a node to the cluster")
	fmt.Println("  remove <server_address> <node_address>  - Remove a node from the cluster")
	fmt.Println("  list <server_address>                   - List all nodes in the cluster")
	os.Exit(1)
}

func addNode(client proto.VideoContentAdminServiceClient, nodeAddr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := client.AddNode(ctx, &proto.AddNodeRequest{
		NodeAddress: nodeAddr,
	})
	if err != nil {
		log.Fatalf("AddNode RPC failed: %v", err)
	}

	fmt.Printf("Successfully added node: %s\n", nodeAddr)
	fmt.Printf("Number of files migrated: %d\n", response.MigratedFileCount)
}

func removeNode(client proto.VideoContentAdminServiceClient, nodeAddr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := client.RemoveNode(ctx, &proto.RemoveNodeRequest{
		NodeAddress: nodeAddr,
	})
	if err != nil {
		log.Fatalf("RemoveNode RPC failed: %v", err)
	}

	fmt.Printf("Successfully removed node: %s\n", nodeAddr)
	fmt.Printf("Number of files migrated: %d\n", response.MigratedFileCount)
}

func listNodes(client proto.VideoContentAdminServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := client.ListNodes(ctx, &proto.ListNodesRequest{})
	if err != nil {
		log.Fatalf("ListNodes RPC failed: %v", err)
	}

	fmt.Println("Storage cluster nodes:")
	if len(response.Nodes) == 0 {
		fmt.Println("  No nodes in cluster")
	} else {
		for _, node := range response.Nodes {
			fmt.Printf("  - %s\n", node)
		}
	}
}
