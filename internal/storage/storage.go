// Lab 8: Implement a network video content service (server)

package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	pb "tritontube/internal/proto"
)

// Implement a network video content service (server)

type StorageServer struct {
	pb.UnimplementedStorageServiceServer
	BaseDir string
}

func NewStorageServer(baseDir string) (*StorageServer, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %v", err)
	}

	return &StorageServer{
		BaseDir: baseDir,
	}, nil
}

func (s *StorageServer) AddFile(ctx context.Context, req *pb.AddFileRequest) (*pb.AddFileResponse, error) {
	var errorString string
	if req.VideoId == "" {
		errorString = "video ID must not be empty"
		return &pb.AddFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	if req.Filename == "" {
		errorString = "filename must not be empty"
		return &pb.AddFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	if len(req.Content) == 0 {
		errorString = "empty content, file must have non-zero content length"
		return &pb.AddFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	videoDir := path.Join(s.BaseDir, req.VideoId)
	filePath := path.Join(videoDir, req.Filename)
	err := os.MkdirAll(videoDir, 0755)
	if err != nil {
		errorString = fmt.Sprintf("error creating directory, %v", err)
		return &pb.AddFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	err = os.WriteFile(filePath, req.Content, 0755)
	if err != nil {
		errorString = fmt.Sprintf("error writing content to file, %v", err)
		return &pb.AddFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}
	return &pb.AddFileResponse{
		Success: true,
		Error:   "",
	}, nil
}

func (s *StorageServer) GetFile(ctx context.Context, req *pb.GetFileRequest) (*pb.GetFileResponse, error) {
	var errorString string
	if req.VideoId == "" {
		errorString = "video ID must not be empty"
		return &pb.GetFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	if req.Filename == "" {
		errorString = "filename must not be empty"
		return &pb.GetFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	filePath := path.Join(s.BaseDir, req.VideoId, req.Filename)
	content, err := os.ReadFile(filePath)
	if err != nil {
		errorString = fmt.Sprintf("error writing content to file, %v", err)
		return &pb.GetFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	return &pb.GetFileResponse{
		Success: true,
		Content: content,
	}, nil
}

func (s *StorageServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	var errorString string
	if req.VideoId == "" {
		errorString = "video ID must not be empty"
		return &pb.DeleteFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	if req.Filename == "" {
		errorString = "filename must not be empty"
		return &pb.DeleteFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}
	filePath := path.Join(s.BaseDir, req.VideoId, req.Filename)

	if _, err := os.Stat(filePath); err != nil {
		errorString = fmt.Sprintf("error doing Stat on file %v: %v", filePath, err)
		return &pb.DeleteFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	err := os.Remove(filePath)
	if err != nil {
		errorString = fmt.Sprintf("failed to delete file %v: %v", filePath, err)
		return &pb.DeleteFileResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	return &pb.DeleteFileResponse{
		Success: true,
	}, nil
}

func (s *StorageServer) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	var errorString string
	if req.VideoId == "" {
		errorString = "video ID must not be empty"
		return &pb.ListFilesResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	videoDir := path.Join(s.BaseDir, req.VideoId)
	if _, err := os.Stat(videoDir); os.IsNotExist(err) {
		// Video directory doesn't exist, return empty list
		return &pb.ListFilesResponse{
			Files: []string{},
		}, nil
	}

	// Read directory contents
	entries, err := os.ReadDir(videoDir)
	if err != nil {
		return &pb.ListFilesResponse{
			Error: fmt.Sprintf("failed to read video directory: %v", err),
		}, nil
	}

	var filenames []string
	for _, entry := range entries {
		if !entry.IsDir() {
			filenames = append(filenames, entry.Name())
		}
	}

	return &pb.ListFilesResponse{
		Files: filenames,
	}, nil
}

func (s *StorageServer) ListAllFiles(ctx context.Context, req *pb.ListAllFilesRequest) (*pb.ListAllFilesResponse, error) {
	var allFiles []*pb.FileInfo
	err := filepath.WalkDir(s.BaseDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip files/dirs that can't be accessed
		}

		if d.IsDir() {
			return nil // ignore directories while doing the listing
		}

		relPath, err := filepath.Rel(s.BaseDir, path)
		if err != nil {
			return nil // Skip files for which the relative path finding fails
		}

		// Split the relative path to get the video ID and filename
		parts := strings.Split(filepath.ToSlash(relPath), "/")
		if len(parts) != 2 {
			return nil // Skip files not in expected videoId/filename structure
		}

		videoId := parts[0]
		fileName := parts[1]

		info, err := d.Info()
		if err != nil {
			return nil // Skip files we can't get info for
		}

		fileInfo := &pb.FileInfo{
			VideoId:  videoId,
			Filename: fileName,
			Size:     info.Size(),
		}

		allFiles = append(allFiles, fileInfo)
		return nil
	})
	if err != nil {
		errorString := fmt.Sprintf("failed to list all files on this server, %v", err)
		return &pb.ListAllFilesResponse{
			Success: false,
			Error:   errorString,
		}, errors.New(errorString)
	}

	return &pb.ListAllFilesResponse{
		Success: true,
		Files:   allFiles,
	}, nil
}
