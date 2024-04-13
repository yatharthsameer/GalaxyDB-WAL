package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
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

func writeDataToShard(db *sql.DB, request WriteRequest) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, entry := range request.Data {
		_, err := tx.Exec("INSERT INTO "+request.Shard+" (Stud_id, Stud_name, Stud_marks) VALUES (?, ?, ?)",
			entry.StudentID, entry.StudentName, entry.StudentMarks)
		if err != nil {
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
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

func replicateWritesToSecondaries(payload WriteRequest, secondaryServers []int) ([]bool, error) {

	acks := make([]bool, len(secondaryServers))
	var err error

	for i, serverID := range secondaryServers {
		payloadData, err := json.Marshal(payload)
		if err != nil {
			acks[i] = false
			continue
		}

		_, err = http.Post(fmt.Sprintf("http://Server%d:5000/write", serverID), "application/json", bytes.NewBuffer(payloadData))
		if err != nil {
			acks[i] = false
		}
	}
	return acks, err
}

func receivedMajorityAck(acks []bool) bool {

	finalAck := true
	for _, ack := range acks {
		if !ack {
			finalAck = false
			break
		}
	}
	return finalAck
}

func isPrimary(primary int) bool {

	serverID := os.Getenv("id")
	serverIDInt, err := strconv.Atoi(serverID)
	if err != nil {
		return false
	}

	return serverIDInt == primary
}

func getWalLength() int {

	_, err := os.Stat(WAL_DIRECTORY_PATH)
	if os.IsNotExist(err) {
		return 0
	}

	files, err := os.ReadDir(WAL_DIRECTORY_PATH)
	if err != nil {
		log.Println("Error while fetching wal length: " + err.Error())
	}

	return len(files)
}
