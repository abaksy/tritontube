package web

import "time"

type VideoMetadata struct {
	Id         string
	UploadedAt time.Time `json:"uploaded_at"`
}

type VideoMetadataService interface {
	Read(id string) (*VideoMetadata, error)
	List() ([]VideoMetadata, error)
	Create(videoId string, uploadedAt time.Time) error
	Close() error
}

type VideoContentService interface {
	Read(videoId string, filename string) ([]byte, error)
	Write(videoId string, filename string, data []byte) error
}
