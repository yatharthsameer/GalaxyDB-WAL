package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func fetchDataFromShard(db *sql.DB, query string) ([]ShardData, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var data []ShardData
	for rows.Next() {
		var entry ShardData
		err := rows.Scan(&entry.StudentID, &entry.StudentName, &entry.StudentMarks)
		if err != nil {
			return nil, err
		}
		data = append(data, entry)
	}

	return data, nil
}

func writeDataToShard(db *sql.DB, request WriteRequest) (*WriteResponse, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	log.Println("writing data to shard")

	for _, entry := range request.Data {
		_, err := tx.Exec("INSERT INTO "+request.Shard+" (Stud_id, Stud_name, Stud_marks) VALUES (?, ?, ?)",
			entry.StudentID, entry.StudentName, entry.StudentMarks)
		if err != nil {
			return nil, err
		}
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	updatedIndex := request.CurrIndex + len(request.Data)

	return &WriteResponse{
		Message:    "Data entries added",
		CurrentIdx: updatedIndex,
		Status:     "success",
	}, nil
}

func writeToWAL(req WriteRequest) error {
	record := WALRecord{
		Timestamp: time.Now(),
		Shard:     req.Shard,
		Data:      req.Data,
	}

	recordData, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("error marshaling WAL record: %w", err)
	}

	err = os.MkdirAll(WAL_DIRECTORY_PATH, 0755)
	if err != nil {
		return fmt.Errorf("error creating WAL directory: %w", err)
	}

	walFileName := fmt.Sprintf("%s_%d.wal", req.Shard, time.Now().UnixNano())
	walFilePath := filepath.Join(WAL_DIRECTORY_PATH, walFileName)

	walFile, err := os.OpenFile(walFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening WAL file: %w", err)
	}
	defer walFile.Close()

	_, err = walFile.Write(recordData)
	if err != nil {
		return fmt.Errorf("error writing to WAL file: %w", err)
	}

	err = walFile.Sync()
	if err != nil {
		return fmt.Errorf("error flushing WAL file: %w", err)
	}

	return nil
}

func replicateWritesToSecondaries(req WriteRequest) ([]bool, error) {

	return nil, nil
}

func receivedMajorityAck(acks []bool) bool {

	return true
}

func commitToDatabase(req WriteRequest) error {

	return nil
}

func isPrimary(serverID int, shardID string) bool {
	// check from MapT
	payload := IsPRimaryRequest{
		ServerID: serverID,
		ShardID:  shardID,
	}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("Error marshaling JSON: ", err)
	}
	req, err := http.NewRequest("GET", LOADBALANCER_URL+"/isPrimary", bytes.NewBuffer(payloadData))
	if err != nil {
		log.Fatal(err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("Error reading response body:", err)
	}

	var isPrimary bool
	err = json.Unmarshal(body, &isPrimary)
	if err != nil {
		log.Println("Error unmarshaling JSON: ", err)
	}
	return isPrimary
}

func writeHandlerNew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	var reqBody WriteRequest
	err := json.NewDecoder(r.Body).Decode(&reqBody)
	if err != nil {
		http.Error(w, "Error decoding JSON", http.StatusBadRequest)
		return
	}

	if err := writeToWAL(reqBody); err != nil {
		http.Error(w, "Error writing to WAL", http.StatusInternalServerError)
		return
	}
	serverID := os.Getenv("SERVER_ID")
	serverIDInt, err := strconv.Atoi(serverID)
	if err != nil {
		http.Error(w, "Error converting serverID to int", http.StatusInternalServerError)
		return
	}

	if isPrimary(serverIDInt, reqBody.Shard) {
		acks, err := replicateWritesToSecondaries(reqBody)
		if err != nil {
			http.Error(w, "Error replicating to secondaries", http.StatusInternalServerError)
			return
		}

		if !receivedMajorityAck(acks) {
			http.Error(w, "Did not receive majority acknowledgments", http.StatusInternalServerError)
			return
		}
	}

	if err := commitToDatabase(reqBody); err != nil {
		http.Error(w, "Error committing to database", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(WriteResponse{
		Message:    "Data entries added",
		CurrentIdx: reqBody.CurrIndex + len(reqBody.Data),
		Status:     "success",
	})
}
