// Lab 7: Implement a local filesystem video content service

package web

import (
	"fmt"
	"log"
	"os"
	"path"
)

// FSVideoContentService implements VideoContentService using the local filesystem.
type FSVideoContentService struct {
	baseDir string
}

// Uncomment the following line to ensure FSVideoContentService implements VideoContentService
// var _ VideoContentService = (*FSVideoContentService)(nil)

func (vcs *FSVideoContentService) Read(videoId string, filename string) ([]byte, error) {
	fullPath := path.Join(vcs.baseDir, videoId, filename)

	videoBuffer, err := os.ReadFile(fullPath)
	if err != nil {
		return nil, fmt.Errorf("error while reading file %v: %v", filename, err)
	}

	return videoBuffer, nil
}

func (vcs *FSVideoContentService) Write(videoId string, filename string, data []byte) error {

	videoDirPath := path.Join(vcs.baseDir, videoId)
	fullPath := path.Join(videoDirPath, filename)

	err := os.MkdirAll(videoDirPath, 0755)
	if err != nil {
		return fmt.Errorf("error creating directory %v: %v", videoDirPath, err)
	}

	fp, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file %v: %v", fullPath, err)
	}
	defer fp.Close()

	_, err = fp.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write to file %v: %v", fullPath, err)
	}
	fp.Sync() // flush in-memory data to disk

	// log.Printf("wrote %v bytes to %v", n, fullPath)

	return nil
}

func NewFSVideoContentService(baseDir string) *FSVideoContentService {
	log.Printf("creating directory %v", baseDir)
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		log.Printf("error in directory creation: %v", err)
		return nil
	}

	return &FSVideoContentService{
		baseDir: baseDir,
	}
}
