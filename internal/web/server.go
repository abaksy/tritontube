// Lab 7: Implement a web server

package web

import (
	"bytes"
	"database/sql"
	"fmt"
	"html/template"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type server struct {
	Addr string
	Port int

	metadataService VideoMetadataService
	contentService  VideoContentService

	mux *http.ServeMux
}

func NewServer(
	metadataService VideoMetadataService,
	contentService VideoContentService,
) *server {
	return &server{
		metadataService: metadataService,
		contentService:  contentService,
	}
}

type VideoTemplate struct {
	Id         string
	EscapedId  string
	UploadTime string
}

func (s *server) Start(lis net.Listener) error {
	s.mux = http.NewServeMux()
	s.mux.HandleFunc("/upload", s.handleUpload)
	s.mux.HandleFunc("/videos/", s.handleVideo)
	s.mux.HandleFunc("/content/", s.handleVideoContent)
	s.mux.HandleFunc("/", s.handleIndex)

	return http.Serve(lis, s.mux)
}

func (s *server) handleIndex(w http.ResponseWriter, r *http.Request) {
	// Get videos from metadata service
	videos, err := s.metadataService.List()
	if err != nil {
		if err != sql.ErrNoRows {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	var templateVideosData []VideoTemplate
	// Escape the URL chracters
	for _, vidMetadata := range videos {
		templateVideosData = append(templateVideosData, VideoTemplate{
			Id:         vidMetadata.Id,
			EscapedId:  url.PathEscape(vidMetadata.Id),
			UploadTime: vidMetadata.UploadedAt.Format("2006-01-02 15:04:05"),
		})
	}

	tmpl, err := template.New("index_page").Parse(indexHTML)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusOK)

	if err = tmpl.Execute(w, templateVideosData); err != nil {
		log.Panicf("failed to render template %v: %v", tmpl.Name(), err)
	}
}

func (s *server) handleUpload(w http.ResponseWriter, r *http.Request) {
	// Check if the request is a POST request
	log.Printf("inside handleUpload handler")
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse multipart form data (32MB max size)
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, "failed to parse form", http.StatusBadRequest)
		return
	}

	// Get the uploaded file
	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "no file uploaded", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Check if the file is an MP4
	if !strings.HasSuffix(strings.ToLower(header.Filename), ".mp4") {
		http.Error(w, "only MP4 files are supported", http.StatusBadRequest)
		return
	}

	// Extract video ID from filename (remove .mp4 extension)
	videoId := strings.TrimSuffix(header.Filename, ".mp4")
	videoId = strings.TrimSuffix(videoId, filepath.Ext(videoId)) // Remove any extension if present

	// Check if the video ID already exists
	existingVideo, err := s.metadataService.Read(videoId)
	if err != nil {
		if err != sql.ErrNoRows {
			http.Error(w, fmt.Sprintf("failed to check for existing video: %v", err), http.StatusInternalServerError)
			return
		}
	}
	if existingVideo != nil {
		http.Error(w, fmt.Sprintf("a video with ID %v already exists", existingVideo.Id), http.StatusConflict)
		return
	}

	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "tritontube-uploads-*")
	if err != nil {
		http.Error(w, "failed to create temp directory", http.StatusInternalServerError)
		return
	}
	defer os.RemoveAll(tempDir) // Clean up temp directory when done

	log.Printf("creating temp file to store MP4 content")
	// Create a temporary file for the uploaded MP4
	mp4Path := filepath.Join(tempDir, header.Filename)
	tempFile, err := os.Create(mp4Path)
	if err != nil {
		http.Error(w, "failed to create temp file", http.StatusInternalServerError)
		return
	}

	// Copy the uploaded file to the temporary file
	if _, err := io.Copy(tempFile, file); err != nil {
		tempFile.Close()
		http.Error(w, "failed to save uploaded file", http.StatusInternalServerError)
		return
	}
	tempFile.Close()

	log.Printf("starting video processing using ffmpeg")
	// Set up the path for the manifest file
	manifestPath := filepath.Join(tempDir, "manifest.mpd")

	// Run FFmpeg to convert MP4 to MPEG-DASH
	cmd := exec.Command("ffmpeg",
		"-i", mp4Path, // input file
		"-c:v", "libx264", // video codec
		"-c:a", "aac", // audio codec
		"-bf", "1", // max 1 b-frame
		"-keyint_min", "120", // minimum keyframe interval
		"-g", "120", // keyframe every 120 frames
		"-sc_threshold", "0", // scene change threshold
		"-b:v", "3000k", // video bitrate
		"-b:a", "128k", // audio bitrate
		"-f", "dash", // dash format
		"-use_timeline", "1", // use timeline
		"-use_template", "1", // use template
		"-init_seg_name", "init-$RepresentationID$.m4s", // init segment naming
		"-media_seg_name", "chunk-$RepresentationID$-$Number%05d$.m4s", // media segment naming
		"-seg_duration", "4", // segment duration in seconds
		manifestPath) // output file

	// Capture FFmpeg output for logging
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run the FFmpeg command
	if err := cmd.Run(); err != nil {
		log.Printf("FFmpeg stdout: %s", stdout.String())
		log.Printf("FFmpeg stderr: %s", stderr.String())
		http.Error(w, fmt.Sprintf("failed to convert video: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("finished video processing")

	// Store all generated files to the content service
	files, err := os.ReadDir(tempDir)
	if err != nil {
		http.Error(w, "failed to read converted files", http.StatusInternalServerError)
		return
	}

	// Upload each generated file to the content service
	for _, file := range files {
		// Skip the original MP4 file
		if file.Name() == header.Filename {
			continue
		}

		// Read the file content
		filePath := filepath.Join(tempDir, file.Name())
		fileContent, err := os.ReadFile(filePath)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to read file %s: %v", file.Name(), err), http.StatusInternalServerError)
			return
		}

		// Store the file in the content service
		if err := s.contentService.Write(videoId, file.Name(), fileContent); err != nil {
			http.Error(w, fmt.Sprintf("failed to store file %s: %v", file.Name(), err), http.StatusInternalServerError)
			return
		}
	}

	// Create metadata entry for the new video
	if err := s.metadataService.Create(videoId, time.Now()); err != nil {
		http.Error(w, fmt.Sprintf("failed to create video metadata: %v", err), http.StatusInternalServerError)
		return
	}

	// Redirect to the home page with status 303 See Other
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *server) handleVideo(w http.ResponseWriter, r *http.Request) {
	videoId := r.URL.Path[len("/videos/"):]
	log.Println("Video ID:", videoId)

	// panic("Lab 7: not implemented")

	tmpl, err := template.New("video-player").Parse(videoHTML)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to render video-player template: %v", err), http.StatusInternalServerError)
		return
	}

	vidData, err := s.metadataService.Read(videoId)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get video %v metadata: %v", videoId, err), http.StatusInternalServerError)
		return
	}

	templateVidData := struct {
		Id         string
		UploadedAt string
	}{
		Id:         videoId,
		UploadedAt: vidData.UploadedAt.Format("2006-01-02 15:04:05"),
	}

	if err := tmpl.Execute(w, templateVidData); err != nil {
		http.Error(w, fmt.Sprintf("failed to render video player template: %v", err), http.StatusInternalServerError)
	}
}

func (s *server) handleVideoContent(w http.ResponseWriter, r *http.Request) {
	// parse /content/<videoId>/<filename>
	videoId := r.URL.Path[len("/content/"):]
	parts := strings.Split(videoId, "/")
	if len(parts) != 2 {
		http.Error(w, "Invalid content path", http.StatusBadRequest)
		return
	}
	videoId = parts[0]
	filename := parts[1]
	log.Println("Video ID:", videoId, "Filename:", filename)
	// panic("Lab 7: not implemented")

	bytes, err := s.contentService.Read(videoId, filename)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read file %v for video %v: %v", filename, videoId, err), http.StatusInternalServerError)
		return
	}

	contentType := getContentType(filename)

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(bytes)))

	// Write the file content to the response
	if _, err := w.Write(bytes); err != nil {
		http.Error(w, fmt.Sprintf("failed to write bytes to network: %v", err), http.StatusInternalServerError)
	}
}

// NOTE: this is a private method that
// does not belong to the "server" type
func getContentType(filename string) string {
	switch {
	case strings.HasSuffix(filename, ".mpd"):
		return "application/dash+xml"
	case strings.HasSuffix(filename, ".m4s"):
		if strings.Contains(filename, "audio") {
			return "audio/mp4"
		}
		return "video/mp4"
	case strings.HasSuffix(filename, ".mp4"):
		return "video/mp4"
	default:
		// Default to octet-stream for unknown types
		return "application/octet-stream"
	}
}
