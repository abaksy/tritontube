// Lab 7: Implement a web server

package web

import (
	"html/template"
	"log"
	"net"
	"net/http"
	"net/url"
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
	VideoId    string
	EscapedId  string
	UploadTime time.Time
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var templateVideosData []VideoTemplate
	// Escape the URL chracters
	for _, vidMetadata := range videos {
		templateVideosData = append(templateVideosData, VideoTemplate{
			VideoId:    vidMetadata.Id,
			EscapedId:  url.PathEscape(vidMetadata.Id),
			UploadTime: vidMetadata.UploadedAt,
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
	panic("Lab 7: not implemented")
}

func (s *server) handleVideo(w http.ResponseWriter, r *http.Request) {
	videoId := r.URL.Path[len("/videos/"):]
	log.Println("Video ID:", videoId)

	panic("Lab 7: not implemented")
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
	panic("Lab 7: not implemented")
}
