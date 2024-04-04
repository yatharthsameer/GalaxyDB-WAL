package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

type ConfigPayload struct {
	Schema schema   `json:"schema"`
	Shards []string `json:"shards"`
}

type schema struct {
	Columns []string `json:"columns"`
	Dtypes  []string `json:"dtypes"`
}

type CopyRequest struct {
	Shards []string `json:"shards"`
}

type ShardData struct {
	StudentID    int    `json:"Stud_id"`
	StudentName  string `json:"Stud_name"`
	StudentMarks int    `json:"Stud_marks"`
}

type WriteRequest struct {
	Shard     string      `json:"shard"`
	CurrIndex int         `json:"curr_idx"`
	Data      []ShardData `json:"data"`
}

type WriteResponse struct {
	Message    string `json:"message"`
	CurrentIdx int    `json:"current_idx"`
	Status     string `json:"status"`
}

type ReadRequest struct {
	Shard  string `json:"shard"`
	StudID struct {
		Low  int `json:"low"`
		High int `json:"high"`
	} `json:"Stud_id"`
}

type ReadResponse struct {
	Data   []ShardData `json:"data"`
	Status string      `json:"status"`
}

type UpdateRequest struct {
	Shard  string    `json:"shard"`
	StudID int       `json:"Stud_id"`
	Data   ShardData `json:"data"`
}

type DeleteRequest struct {
	Shard  string `json:"shard"`
	StudID int    `json:"Stud_id"`
}

func heartbeatEndpoint(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
}

func configEndpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	// Decode the request body
	var reqBody ConfigPayload
	err := json.NewDecoder(r.Body).Decode(&reqBody)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Error decoding JSON: %v", err)
		return
	}

	// fmt.Println("Columns:", reqBody.Schema.Columns)
	// fmt.Println("Data Types:", reqBody.Schema.Dtypes)
	// fmt.Println("Shards:", reqBody.Shards)

	var resMsg string
	serverId := fmt.Sprintf("Server%s", os.Getenv("id"))
	numberShards := len(reqBody.Shards)
	for i, shard := range reqBody.Shards {
		resMsg += fmt.Sprintf("%s:%s", serverId, shard)
		if i == numberShards-1 {
			resMsg += " configured"
		} else {
			resMsg += ", "
		}
	}

	// initialize the shard tables in server database
	for _, shard := range reqBody.Shards {
		query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s ( ", shard)
		for i, col := range reqBody.Schema.Columns {
			query += fmt.Sprintf("%s %s", col, reqBody.Schema.Dtypes[i])
			if i < len(reqBody.Schema.Columns)-1 {
				query += ", "
			}
		}
		query += ")"
		_, err = db.Exec(query)
		if err != nil {
			log.Fatalf("error creating table: %s", err)
		}
	}

	// Send response
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	resp := make(map[string]string)

	resp["message"] = resMsg
	resp["status"] = "success"
	jsonResp, err := json.Marshal(resp)
	if err != nil {
		log.Fatalf("error in JSON marshal: %s", err)
	}
	w.Write(jsonResp)
}

// function to execute the query and return data from the shard
func fetchDataFromShard(query string) ([]ShardData, error) {
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

func copyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	// Decode the request parameters
	var reqBody CopyRequest
	err := json.NewDecoder(r.Body).Decode(&reqBody)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Error decoding JSON: %v", err)
		return
	}

	resp := make(map[string]interface{})

	for _, shard := range reqBody.Shards {
		query := fmt.Sprintf("SELECT Stud_id, Stud_name, Stud_marks FROM %s", shard)
		data, err := fetchDataFromShard(query)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Error fetching data from shard %s: %v", shard, err)
			return
		}
		resp[shard] = data
	}
	resp["status"] = "success"
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func writeDataToShard(request WriteRequest) (*WriteResponse, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	fmt.Println("writing data to shard")

	// insert each entry into the shard table
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

	// update the current index for the shard
	updatedIndex := request.CurrIndex + len(request.Data)

	return &WriteResponse{
		Message:    "Data entries added",
		CurrentIdx: updatedIndex,
		Status:     "success",
	}, nil
}

func writeHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Starting serve hiiiiiiiiiiiiiiii")

	fmt.Println("Starting serve hiiiiiiiiiiiiiiii")

	if r.Method != http.MethodPost {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}


	var reqBody WriteRequest
	err := json.NewDecoder(r.Body).Decode(&reqBody)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Error decoding JSON: %v", err)
		return
	}
	if err := writeToWAL(reqBody); err != nil {
		http.Error(w, "Error writing to WAL", http.StatusInternalServerError)
		return
	}
	fmt.Println("Data written to WAL 1")
	
	fmt.Println("Data written to WAL 2")

	resp, err := writeDataToShard(reqBody)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error writing data to shard: %v", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func readHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}
	log.Println("Server image built readHandler")

	var reqBody ReadRequest
	err := json.NewDecoder(r.Body).Decode(&reqBody)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Error decoding JSON: %v", err)
		return
	}
	shard := reqBody.Shard
	query := fmt.Sprintf("SELECT Stud_id, Stud_name, Stud_marks FROM %s WHERE Stud_id BETWEEN %d AND %d", shard, reqBody.StudID.Low, reqBody.StudID.High)
	data, err := fetchDataFromShard(query)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error reading data from shard %s: %v", shard, err)
		return
	}

	response := ReadResponse{
		Data:   data,
		Status: "success",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}
func writeToWAL(req WriteRequest) error {
    log.Println("Starting to write to WAL") // Log that we're starting the WAL write process
    
    // Create a WAL record
    record := WALRecord{
        Timestamp: time.Now(),
        Shard:     req.Shard,
        Data:      req.Data,
    }
log.Println("Server image built writeToWAL")
    // Marshal the record to JSON
    recordData, err := json.Marshal(record)
    if err != nil {
        log.Printf("Error marshaling WAL record: %v\n", err) // Log marshaling errors
        return fmt.Errorf("error marshaling WAL record: %w", err)
    }

    // Ensure the WAL directory exists
    walDir := "wal_logs"
    err = os.MkdirAll(walDir, 0755)
    if err != nil {
        log.Printf("Error creating WAL directory '%s': %v\n", walDir, err) // Log directory creation errors
        return fmt.Errorf("error creating WAL directory: %w", err)
    }

    // Create a unique file name for the WAL record
    walFileName := fmt.Sprintf("%s_%d.wal", req.Shard, time.Now().UnixNano())
    walFilePath := filepath.Join(walDir, walFileName)
    log.Printf("WAL file path: %s\n", walFilePath) // Log the WAL file path

    // Open the WAL file for writing
    walFile, err := os.OpenFile(walFilePath, os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Printf("Error opening WAL file '%s': %v\n", walFilePath, err) // Log file opening errors
        return fmt.Errorf("error opening WAL file: %w", err)
    }
    defer walFile.Close()

    // Write the record to the file
    _, err = walFile.Write(recordData)
    if err != nil {
        log.Printf("Error writing to WAL file: %v\n", err) // Log writing errors
        return fmt.Errorf("error writing to WAL file: %w", err)
    }
    log.Println("WAL write was successful") // Log that the write was successful

    // Flush the file to ensure it's written to disk
    err = walFile.Sync()
    if err != nil {
        log.Printf("Error flushing WAL file: %v\n", err) // Log flushing errors
        return fmt.Errorf("error flushing WAL file: %w", err)
    }
    
    log.Println("WAL sync was successful") // Log that the sync was successful

    return nil
}

func updateHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}
	var reqBody UpdateRequest
	err := json.NewDecoder(r.Body).Decode(&reqBody)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Error decoding JSON: %v", err)
		return
	}
	shard := reqBody.Shard
	query := fmt.Sprintf("UPDATE %s SET Stud_marks = ? WHERE Stud_id = ?", shard)
	_, err = db.Exec(query, reqBody.Data.StudentMarks, reqBody.StudID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error updating data in shard %s for Stud_id %d: %v", shard, reqBody.StudID, err)
		return
	}
	resp := make(map[string]string)
	resp["message"] = fmt.Sprintf("Data entry for Stud_id:%d updated", reqBody.StudID)
	resp["status"] = "success"
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}
	var reqBody DeleteRequest
	err := json.NewDecoder(r.Body).Decode(&reqBody)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Error decoding JSON: %v", err)
		return
	}
	shard := reqBody.Shard
	query := fmt.Sprintf("DELETE FROM %s WHERE Stud_id = ?", shard)
	_, err = db.Exec(query, reqBody.StudID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error deleting data in shard %s for Stud_id %d: %v", reqBody.Shard, reqBody.StudID, err)
		return
	}
	resp := make(map[string]string)
	resp["message"] = fmt.Sprintf("Data entry with Stud_id:%d removed", reqBody.StudID)
	resp["status"] = "success"
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
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

	// Step 1: Write changes to WAL
	if err := writeToWAL(reqBody); err != nil {
		http.Error(w, "Error writing to WAL", http.StatusInternalServerError)
		return
	}

	// Check if this server is the primary server for the shard
	if isPrimary(reqBody.Shard) {
		// Step 2: Replicate changes to secondary servers
		acks, err := replicateWritesToSecondaries(reqBody)
		if err != nil {
			http.Error(w, "Error replicating to secondaries", http.StatusInternalServerError)
			return
		}

		// Step 3: Wait for acknowledgments from the majority of replicas
		if !receivedMajorityAck(acks) {
			http.Error(w, "Did not receive majority acknowledgments", http.StatusInternalServerError)
			return
		}
	}

	// Step 4: Commit to database
	if err := commitToDatabase(reqBody); err != nil {
		http.Error(w, "Error committing to database", http.StatusInternalServerError)
		return
	}

	// Respond with success message
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(WriteResponse{
		Message:    "Data entries added",
		CurrentIdx: reqBody.CurrIndex + len(reqBody.Data), // Update the current index
		Status:     "success",
	})
}

// WALRecord represents a record in the WAL file.
type WALRecord struct {
	Timestamp time.Time   `json:"timestamp"`
	Shard     string      `json:"shard"`
	Data      []ShardData `json:"data"`
}

// writeToWAL writes the pending writes to the WAL file.

// replicateWritesToSecondaries sends the write request to secondary servers.
// This is a simplified version. In a real implementation, you would use a network protocol.
func replicateWritesToSecondaries(req WriteRequest) ([]bool, error) {
	// Pseudo-code: Send replication requests to secondary servers
	// ... Implement replication logic ...

	return nil, nil // return actual acknowledgments and errors
}

// receivedMajorityAck checks if the majority of acknowledgments have been received.
func receivedMajorityAck(acks []bool) bool {
	// Pseudo-code: Determine if a majority of "true" acknowledgments have been received
	// ... Implement majority check logic ...

	return true // return true if majority is received
}

// commitToDatabase applies the write to the database.
func commitToDatabase(req WriteRequest) error {
	// Pseudo-code: Commit writes to the database
	// ... Implement database commit logic ...

	return nil // return error if something goes wrong
}

// isPrimary checks if the server is the primary server for the shard.
func isPrimary(shardID string) bool {
	// Pseudo-code: Determine if this server is the primary for the given shard
	// ... Implement primary server check logic ...

	return true // return true if this is the primary server
}
func main() {
	log.SetOutput(os.Stdout) // Set the log output to stdout

	var err error
	db, err = sql.Open("sqlite3", "galaxy.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/heartbeat", heartbeatEndpoint)
	http.HandleFunc("/config", configEndpoint)
	http.HandleFunc("/copy", copyHandler)
	http.HandleFunc("/read", readHandler)
	http.HandleFunc("/write", writeHandler)
	http.HandleFunc("/update", updateHandler)
	http.HandleFunc("/delete", deleteHandler)
fmt.Println("Starting serve hiiiiiiiiiiiiiiii")
	fmt.Println("Starting server on port 5000")
	err = http.ListenAndServe(":5000", nil)
	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		panic(err)
	}
}
