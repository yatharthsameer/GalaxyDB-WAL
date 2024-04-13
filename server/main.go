package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

func heartbeatHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
}

func configHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	var reqBody ConfigPayload
	err := json.NewDecoder(r.Body).Decode(&reqBody)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Error decoding JSON: %v", err)
		return
	}

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

func copyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

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
		data, err := fetchDataFromShard(db, query)
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

// func writeHandler(w http.ResponseWriter, r *http.Request) {
// 	if r.Method != http.MethodPost {
// 		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
// 		return
// 	}

// 	var reqBody WriteRequest
// 	err := json.NewDecoder(r.Body).Decode(&reqBody)
// 	if err != nil {
// 		w.WriteHeader(http.StatusBadRequest)
// 		fmt.Fprintf(w, "Error decoding JSON: %v", err)
// 		return
// 	}

// 	if err := writeToWAL(reqBody); err != nil {
// 		http.Error(w, "Error writing to WAL", http.StatusInternalServerError)
// 		return
// 	}

// 	resp, err := writeDataToShard(db, reqBody)
// 	if err != nil {
// 		w.WriteHeader(http.StatusInternalServerError)
// 		fmt.Fprintf(w, "Error writing data to shard: %v", err)
// 		return
// 	}

// 	w.Header().Set("Content-Type", "application/json")
// 	w.WriteHeader(http.StatusOK)
// 	json.NewEncoder(w).Encode(resp)
// }

func writeHandler(w http.ResponseWriter, r *http.Request) {
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

	payload := ShardServersRequest{
		ShardID: reqBody.Shard,
	}

	payloadData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("Error marshaling JSON: ", err)
	}

	req, err := http.NewRequest("GET", SHARD_MANAGER_URL+"/shard_servers", bytes.NewBuffer(payloadData))
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

	var shardServers ShardServersResponse
	err = json.Unmarshal(body, &shardServers)
	if err != nil {
		log.Println("Error unmarshaling JSON: ", err)
	}

	if err := writeToWAL(reqBody); err != nil {
		http.Error(w, "Error writing to WAL", http.StatusInternalServerError)
		return
	}

	if isPrimary(shardServers.Primary) {

		var secondaries []int
		for _, server := range shardServers.ServerIDs {
			if server != shardServers.Primary {
				secondaries = append(secondaries, server)
			}
		}

		acks, err := replicateWritesToSecondaries(reqBody, secondaries)
		if err != nil {
			http.Error(w, "Error replicating to secondaries", http.StatusInternalServerError)
			return
		}

		if !receivedMajorityAck(acks) {
			http.Error(w, "Did not receive majority acknowledgments", http.StatusInternalServerError)
			return
		}
	}

	if err := writeDataToShard(db, reqBody); err != nil {
		http.Error(w, "Error committing to database", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(WriteResponse{
		Message: "Data entries added",
		Status:  "success",
	})
}

func readHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	var reqBody ReadRequest
	err := json.NewDecoder(r.Body).Decode(&reqBody)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "Error decoding JSON: %v", err)
		return
	}

	shard := reqBody.Shard
	query := fmt.Sprintf("SELECT Stud_id, Stud_name, Stud_marks FROM %s WHERE Stud_id BETWEEN %d AND %d", shard, reqBody.StudID.Low, reqBody.StudID.High)

	data, err := fetchDataFromShard(db, query)
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

func walLengthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	walLength := getWalLength()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(walLength)
}

func main() {
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

	http.HandleFunc("/heartbeat", heartbeatHandler)
	http.HandleFunc("/config", configHandler)
	http.HandleFunc("/copy", copyHandler)
	http.HandleFunc("/read", readHandler)
	http.HandleFunc("/write", writeHandler)
	http.HandleFunc("/update", updateHandler)
	http.HandleFunc("/delete", deleteHandler)
	http.HandleFunc("/wal_length", walLengthHandler)

	fmt.Println("Starting server on port 5000")
	err = http.ListenAndServe(":5000", nil)

	if errors.Is(err, http.ErrServerClosed) {
		fmt.Printf("server closed\n")
	} else if err != nil {
		fmt.Printf("error starting server: %s\n", err)
		panic(err)
	}
}
