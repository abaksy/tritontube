// Lab 7: Implement a SQLite video metadata service

package web

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const SQLDriver = "sqlite3"

type SQLiteVideoMetadataService struct {
	dbFile string  // database file path
	db     *sql.DB // database connection pool
}

func NewSQLiteVideoMetadataService(dbFile string) (*SQLiteVideoMetadataService, error) {
	dbConn, err := sql.Open(SQLDriver, dbFile)
	if err != nil {
		return nil, err
	}

	return &SQLiteVideoMetadataService{dbFile: dbFile, db: dbConn}, nil
}

// Uncomment the following line to ensure SQLiteVideoMetadataService implements VideoMetadataService
// var _ VideoMetadataService = (*SQLiteVideoMetadataService)(nil)

func (vms *SQLiteVideoMetadataService) Read(videoId string) (*VideoMetadata, error) {
	var metadata VideoMetadata

	SQLQuery := "SELECT * FROM videos WHERE video_id = ?"

	row := vms.db.QueryRow(SQLQuery, videoId)

	err := row.Scan(&metadata.Id, &metadata.UploadedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			log.Printf("query %v for videoID %v returned empty result", SQLQuery, videoId)
		}
		return nil, fmt.Errorf("failed to extract fields from row: %v", err)
	}

	return &metadata, nil
}

func (vms *SQLiteVideoMetadataService) List() ([]VideoMetadata, error) {
	var videos []VideoMetadata

	SQLQuery := "SELECT * FROM videos ORDER BY uploaded_at DESC"

	rows, err := vms.db.Query(SQLQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var videoId string
		var uploadedAt time.Time
		err = rows.Scan(&videoId, &uploadedAt)
		if err != nil {
			log.Fatal(err)
		}
		videos = append(videos, VideoMetadata{Id: videoId, UploadedAt: uploadedAt})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}

	return videos, nil
}

func (vms *SQLiteVideoMetadataService) Create(videoId string, uploadedAt time.Time) error {
	SQLQuery := "INSERT INTO videos(video_id, uploaded_at) VALUES (?, ?)"

	txn, err := vms.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}

	// Build prepared statement
	stmt, err := txn.Prepare(SQLQuery)
	if err != nil {
		return fmt.Errorf("failed to create prepared statement: %v", err)
	}

	// Execute query, populate `?` with parameter values
	_, err = stmt.Exec(videoId, uploadedAt)
	if err != nil {
		return fmt.Errorf("failed to execute stmt: %v", err)
	}

	// Commit update to DB
	err = txn.Commit()
	if err != nil {
		return fmt.Errorf("error while commiting transaction: %v", err)
	}
	return nil
}
