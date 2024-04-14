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

func writeDataToShard(db *sql.DB, request Requester) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	reqData := request.GetShardData()
	reqShard := request.GetShard()
	for _, entry := range reqData {
		_, err := tx.Exec("INSERT INTO "+reqShard+" (Stud_id, Stud_name, Stud_marks) VALUES (?, ?, ?)",
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

func writeToWAL(req Requester) error {
	record := WALRecord{
		Timestamp: time.Now(),
		Shard:     req.GetShard(),
		Data:      req.GetShardData(),
		StudID:    req.GetStudID(),
	}

	recordData, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("error marshaling WAL record: %w", err)
	}

	err = os.MkdirAll(WAL_DIRECTORY_PATH, 0755)
	if err != nil {
		return fmt.Errorf("error creating WAL directory: %w", err)
	}

	walFilePath := filepath.Join(WAL_DIRECTORY_PATH, "wal.txt")

	walFile, err := os.OpenFile(walFilePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("error opening WAL file: %w", err)
	}
	defer walFile.Close()
	newLine := []byte("\n")
	recordData = append(recordData, newLine...)
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

func (u UpdateRequest) GetShard() string {
	return u.Shard
}

func (u UpdateRequest) GetStudID() int {
	return u.StudID
}

func (u UpdateRequest) GetShardData() []ShardData {
	return []ShardData{u.Data}
}

func (d DeleteRequest) GetShard() string {
	return d.Shard
}

func (d DeleteRequest) GetStudID() int {
	return d.StudID
}

func (d DeleteRequest) GetShardData() []ShardData {
	// No data for DeleteRequest
	return nil
}
func (w WriteRequest) GetShard() string {
	return w.Shard
}

func (w WriteRequest) GetStudID() int {
	// No student ID for WriteRequest
	return 0
}

func (w WriteRequest) GetShardData() []ShardData {
	return w.Data
}
func replicateToSecondaries(payload Requester, reqMethod string, route string, secondaryServers []int) ([]bool, error) {

	acks := make([]bool, len(secondaryServers))
	var err error

	for i, serverID := range secondaryServers {
		payloadData, err := json.Marshal(payload)
		if err != nil {
			acks[i] = false
			continue
		}

		req, err := http.NewRequest(reqMethod, fmt.Sprintf("http://Server%d:3000%s", serverID, route), bytes.NewBuffer(payloadData))
		if err != nil {
			log.Println("Error sending request to Server:", err)
			acks[i] = false
		}
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			acks[i] = false
		}
		resp.Body.Close()
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

func lineCounter(r io.Reader) (int, error) {
	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func getWalLength() int {

	walFilePath := filepath.Join(WAL_DIRECTORY_PATH, "wal.txt")
	file, err := os.Open(walFilePath)
	if os.IsNotExist(err) {
		return 0
	}
	defer file.Close()
	lines, err := lineCounter(file)
	if err != nil {
		log.Println("Error while fetching wal file length: " + err.Error())
	}
	return lines
}
