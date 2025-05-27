package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"tritontube/internal/proto"
	"tritontube/internal/storage"

	"google.golang.org/grpc"
)

func main() {
	host := flag.String("host", "localhost", "Host address for the server")
	port := flag.Int("port", 8090, "Port number for the server")
	flag.Parse()

	// Validate arguments
	if *port <= 0 {
		panic("Error: Port number must be positive")
	}

	if flag.NArg() < 1 {
		fmt.Println("Usage: storage [OPTIONS] <baseDir>")
		fmt.Println("Error: Base directory argument is required")
		return
	}
	baseDir := flag.Arg(0)

	fmt.Println("Starting storage server...")
	fmt.Printf("Host: %s\n", *host)
	fmt.Printf("Port: %d\n", *port)
	fmt.Printf("Base Directory: %s\n", baseDir)

	// Create storage directory
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		log.Panicf("failed to start storage server, error in dir. creation: %v", err)
	}

	storageService, err := storage.NewStorageServer(baseDir)
	if err != nil {
		log.Panicf("error creating storageservice: %v", err)
	}

	// Set up gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *host, *port))
	if err != nil {
		log.Fatalf("Failed to listen on %s:%d: %v", *host, *port, err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterStorageServiceServer(grpcServer, storageService)

	log.Printf("Storage server starting on %s:%d", *host, *port)
	log.Printf("Using storage directory: %s", baseDir)

	// Start serving
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve gRPC server: %v", err)
	}

	defer grpcServer.GracefulStop()
}
