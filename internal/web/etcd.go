// Lab 9: Implement a distributed video metadata service

package web

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type EtcdVideoMetadataService struct {
	DBClient  *clientv3.Client
	Endpoints []string
	prefix    string // Prefix to use while getting keys from the DB
}

// Uncomment the following line to ensure EtcdVideoMetadataService implements VideoMetadataService
// var _ VideoMetadataService = (*EtcdVideoMetadataService)(nil)

func NewEtcdVideoMetadataService(endpoints string) (*EtcdVideoMetadataService, error) {
	endpointList := strings.Split(endpoints, ",")

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpointList,
		DialTimeout: 5 * time.Second,
	})

	if err != nil {
		return nil, err
	}

	return &EtcdVideoMetadataService{
		DBClient:  cli,
		Endpoints: endpointList,
		prefix:    "video:",
	}, nil
}

/*
type VideoMetadataService interface {
	Read(id string) (*VideoMetadata, error)
	List() ([]VideoMetadata, error)
	Create(videoId string, uploadedAt time.Time) error
	Close() error
}
*/

func (vms *EtcdVideoMetadataService) Read(id string) (*VideoMetadata, error) {
	var metadata VideoMetadata
	ctx, close := context.WithTimeout(context.Background(), 5*time.Second)
	defer close()

	videoKey := vms.prefix + id
	resp, err := vms.DBClient.Get(ctx, videoKey)
	if err != nil {
		switch err {
		case context.Canceled:
			return nil, fmt.Errorf("ctx is canceled by another routine: %v", err)
		case context.DeadlineExceeded:
			return nil, fmt.Errorf("ctx is attached with a deadline is exceeded: %v", err)
		case rpctypes.ErrEmptyKey:
			return nil, fmt.Errorf("client-side error: %v", err)
		default:
			return nil, fmt.Errorf("bad cluster endpoints, which are not etcd servers: %v", err)
		}
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	if err := json.Unmarshal(resp.Kvs[0].Value, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal video metadata: %w", err)
	}

	metadata.Id = id
	return &metadata, nil
}

func (vms *EtcdVideoMetadataService) List() ([]VideoMetadata, error) {
	ctx, close := context.WithTimeout(context.Background(), 5*time.Second)
	defer close()

	resp, err := vms.DBClient.Get(ctx, vms.prefix, clientv3.WithPrefix())
	if err != nil {
		switch err {
		case context.Canceled:
			return nil, fmt.Errorf("ctx is canceled by another routine: %v", err)
		case context.DeadlineExceeded:
			return nil, fmt.Errorf("ctx is attached with a deadline is exceeded: %v", err)
		case rpctypes.ErrEmptyKey:
			return nil, fmt.Errorf("client-side error: %v", err)
		default:
			return nil, fmt.Errorf("bad cluster endpoints, which are not etcd servers: %v", err)
		}
	}

	videos := make([]VideoMetadata, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var metadata VideoMetadata
		if err := json.Unmarshal(kv.Value, &metadata); err != nil {
			// Log error but continue processing other records
			fmt.Printf("Warning: failed to unmarshal video metadata for key %s: %v\n", string(kv.Key), err)
			continue
		}

		// Extract video ID from key (remove prefix)
		metadata.Id = strings.TrimPrefix(string(kv.Key), vms.prefix)
		videos = append(videos, metadata)
	}

	return videos, nil
}

func (vms *EtcdVideoMetadataService) Create(videoId string, uploadedAt time.Time) error {
	ctx, close := context.WithTimeout(context.Background(), 5*time.Second)
	defer close()

	videoKey := vms.prefix + videoId

	metadata := struct {
		UploadedAt time.Time `json:"uploaded_at"`
	}{
		UploadedAt: uploadedAt,
	}

	value, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	// Check if key already exists to prevent overwriting
	checkResp, err := vms.DBClient.Get(ctx, videoKey)
	if err != nil {
		return fmt.Errorf("failed to check if video exists: %w", err)
	}
	if len(checkResp.Kvs) > 0 {
		return fmt.Errorf("video %s already exists", videoId)
	}

	_, err = vms.DBClient.Put(ctx, videoKey, string(value))
	if err != nil {
		return fmt.Errorf("failed to create metadata entry: %v", err)
	}
	return nil
}

func (vms *EtcdVideoMetadataService) Close() error {
	if err := vms.DBClient.Close(); err != nil {
		return fmt.Errorf("failed to close etcd client conn: %v", err)
	}
	return nil
}

// func main() {
// 	endpoints := "54.187.69.76:2379,35.89.151.238:2379,44.249.34.21:2379"
// 	vms, err := NewEtcdVideoMetadataService(endpoints)
// 	if err != nil {
// 		panic(err)
// 	}

// 	// err = vms.Create("donald-duck", time.Now())
// 	// if err != nil {
// 	// 	panic(err)
// 	// }

// 	data, err := vms.Read("donald-duck")
// 	if err != nil {
// 		panic(err)
// 	}

// 	fmt.Printf("data: %+v\n", data)

// 	vms.Close()
// }
