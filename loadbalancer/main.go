package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	_ "github.com/lib/pq"

	"github.com/yatharthsameer/galaxydb/loadbalancer/internal/consistenthashmap"
)

var (
	schemaConfig  SchemaConfig
	shardTConfigs map[string]ShardTConfig
	serverIDs     []int
	db            *sql.DB
	serverDown    chan int
)

func initHandler(w http.ResponseWriter, r *http.Request) {
	var req InitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	schemaConfig = req.Schema

	for rawServerName, shardIDs := range req.Servers {
		serverID := getServerID(rawServerName)

		for _, shardID := range shardIDs {
			_, err := db.Exec("INSERT INTO mapt (shard_id, server_id) VALUES ($1, $2);", shardID, serverID)
			if err != nil {
				log.Fatal(err)
			}
		}

		serverIDs = append(serverIDs, serverID)

		spawnNewServerInstance(fmt.Sprintf("Server%d", serverID), serverID)
		configNewServerInstance(serverID, shardIDs, req.Schema)
		go checkHeartbeat(serverID, serverDown)
	}

	for _, shard := range req.Shards {
		_, err := db.Exec("INSERT INTO shardt (stud_id_low, shard_id, shard_size, valid_idx) VALUES ($1, $2, $3, $4);", shard.StudIDLow, shard.ShardID, shard.ShardSize, 0)
		if err != nil {
			log.Fatal(err)
		}

		config := shardTConfigs[shard.ShardID]
		config.chm = &consistenthashmap.ConsistentHashMap{}
		config.mutex = &sync.Mutex{}
		shardTConfigs[shard.ShardID] = config

		shardTConfigs[shard.ShardID].chm.Init()
		rows, err := db.Query("SELECT server_id FROM mapt WHERE shard_id = $1;", shard.ShardID)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var serverID int
			err = rows.Scan(&serverID)
			if err != nil {
				log.Fatal(err)
			}
			shardTConfigs[shard.ShardID].chm.AddServer(serverID)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Configured Database", "status": "success"})
}

func statusHandler(w http.ResponseWriter, _ *http.Request) {
	servers := make(map[string][]string)

	for _, serverID := range serverIDs {
		serverName := fmt.Sprintf("Server%d", serverID)
		servers[serverName] = []string{}

		rows, err := db.Query("SELECT shard_id FROM mapt WHERE server_id = $1;", serverID)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var shardID string
			err = rows.Scan(&shardID)
			if err != nil {
				log.Fatal(err)
			}
			servers[serverName] = append(servers[serverName], shardID)
		}
	}

	shards := []Shard{}
	rows, err := db.Query("SELECT stud_id_low, shard_id, shard_size FROM shardt;")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var shard Shard
		err = rows.Scan(&shard.StudIDLow, &shard.ShardID, &shard.ShardSize)
		if err != nil {
			log.Fatal(err)
		}
		shards = append(shards, shard)
	}

	response := map[string]interface{}{
		"N":       len(servers),
		"schema":  schemaConfig,
		"shards":  shards,
		"servers": servers,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func addServersHandler(w http.ResponseWriter, r *http.Request) {
	var req AddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.Servers) < req.N {
		resp := AddResponseFailed{
			Message: "<Error> Number of new servers (n) is greater than newly added instances",
			Status:  "failure",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	serverIDsAdded := []int{}

	for rawServerName, shardIDs := range req.Servers {
		serverID := getServerID(rawServerName)
		serverIDsAdded = append(serverIDsAdded, serverID)

		for _, shardID := range shardIDs {
			_, err := db.Exec("INSERT INTO mapt (shard_id, server_id) VALUES ($1, $2);", shardID, serverID)
			if err != nil {
				log.Fatal(err)
			}
		}

		serverIDs = append(serverIDs, serverID)

		spawnNewServerInstance(fmt.Sprintf("Server%d", serverID), serverID)
		configNewServerInstance(serverID, shardIDs, schemaConfig)
		go checkHeartbeat(serverID, serverDown)
	}

	for _, shard := range req.NewShards {
		_, err := db.Exec("INSERT INTO shardt (stud_id_low, shard_id, shard_size, valid_idx) VALUES ($1, $2, $3, $4);", shard.StudIDLow, shard.ShardID, shard.ShardSize, 0)
		if err != nil {
			log.Fatal(err)
		}

		config := shardTConfigs[shard.ShardID]
		config.chm = &consistenthashmap.ConsistentHashMap{}
		config.mutex = &sync.Mutex{}
		shardTConfigs[shard.ShardID] = config

		shardTConfigs[shard.ShardID].chm.Init()
		rows, err := db.Query("SELECT server_id FROM mapt WHERE shard_id = $1;", shard.ShardID)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var serverID int
			err = rows.Scan(&serverID)
			if err != nil {
				log.Fatal(err)
			}
			shardTConfigs[shard.ShardID].chm.AddServer(serverID)
		}
	}

	addServerMessage := "Add "
	for index, server := range serverIDsAdded {
		addServerMessage = fmt.Sprintf("%sServer:%d", addServerMessage, server)
		if index == len(serverIDsAdded)-1 {
			continue
		} else if index == len(serverIDsAdded)-2 {
			addServerMessage += " and "
		} else {
			addServerMessage += ", "
		}
	}

	response := AddResponseSuccess{
		N:       len(serverIDs),
		Message: addServerMessage,
		Status:  "successful",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func removeServersHandler(w http.ResponseWriter, r *http.Request) {
	var req RemoveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.Servers) > req.N {
		resp := RemoveResponseFailed{
			Message: "<Error> Length of server list is more than removable instances",
			Status:  "failure",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(resp)
		return
	}

	serverIDsRemoved := []int{}
	for _, serverName := range req.Servers {
		serverIDsRemoved = append(serverIDsRemoved, getServerID(serverName))
	}

	additionalRemovalsNeeded := req.N - len(serverIDsRemoved)
	for additionalRemovalsNeeded > 0 {
		if serverID := chooseRandomServerForRemoval(serverIDs, serverIDsRemoved); serverID != -1 {
			serverIDsRemoved = append(serverIDsRemoved, serverID)
			additionalRemovalsNeeded -= 1
		}
	}

	for _, serverIDRemoved := range serverIDsRemoved {
		shardIDsRemoved := []string{}
		rows, err := db.Query("SELECT shard_id FROM mapt WHERE server_id = $1;", serverIDRemoved)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var shardID string
			err = rows.Scan(&shardID)
			if err != nil {
				log.Fatal(err)
			}
			shardIDsRemoved = append(shardIDsRemoved, shardID)
		}

		for _, shardIDRemoved := range shardIDsRemoved {
			shardTConfigs[shardIDRemoved].chm.RemoveServer(serverIDRemoved)
		}

		_, err = db.Exec("DELETE FROM mapt WHERE server_id = $1;", serverIDRemoved)
		if err != nil {
			log.Fatal(err)
		}
	}

	newServerIDs := []int{}
	for _, serverID := range serverIDs {
		isPresent := false
		for _, serverIDRemoved := range serverIDsRemoved {
			if serverIDRemoved == serverID {
				isPresent = true
				break
			}
		}
		if !isPresent {
			newServerIDs = append(newServerIDs, serverID)
		}
	}
	serverIDs = newServerIDs

	serverNamesRemoved := []string{}
	for _, serverIDRemoved := range serverIDsRemoved {
		serverNameRemoved := fmt.Sprintf("Server%d", serverIDRemoved)
		removeServerInstance(serverNameRemoved)

		serverNamesRemoved = append(serverNamesRemoved, serverNameRemoved)
	}

	response := RemoveResponseSuccess{
		Message: map[string]interface{}{
			"N":       len(serverIDs),
			"servers": serverNamesRemoved,
		},
		Status: "successful",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func readHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	var req ReadRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	shardIDsQueried := []string{}
	rows, err := db.Query("SELECT shard_id FROM shardt WHERE (stud_id_low BETWEEN $1 AND $2) OR (stud_id_low+shard_size BETWEEN $1 AND $2);", req.StudID.Low, req.StudID.High)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var shardID string
		err = rows.Scan(&shardID)
		if err != nil {
			log.Fatal(err)
		}
		shardIDsQueried = append(shardIDsQueried, shardID)
	}

	var studData []StudT
	for _, shardIDQueried := range shardIDsQueried {
		payload := ServerReadPayload{
			Shard:  shardIDQueried,
			StudID: req.StudID,
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			log.Fatalln("Error marshaling JSON: ", err)
			return
		}

		serverID := shardTConfigs[shardIDQueried].chm.GetServerForRequest(getRandomID())

		resp, err := http.Post("http://"+getServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(SERVER_PORT)+"/read", "application/json", bytes.NewBuffer(payloadData))
		if err != nil {
			log.Println("Error reading from Server:", err)
			return
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Println("Error reading response body:", err)
		}

		var respData ServerReadResponse
		json.Unmarshal(body, &respData)
		resp.Body.Close()

		studData = append(studData, respData.Data...)
	}

	response := ReadResponse{
		ShardsQueried: shardIDsQueried,
		Data:          studData,
		Status:        "success",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func WriteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	var req WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	studDataToWrite := map[string][]StudT{}
	for _, studData := range req.Data {
		shardID := getShardIDFromStudID(db, studData.StudID)
		studDataToWrite[shardID] = append(studDataToWrite[shardID], studData)
	}

	for shardID, studData := range studDataToWrite {
		shardTConfigs[shardID].mutex.Lock()

		currentIndex := getValidIDx(db, shardID)

		payload := ServerWritePayload{
			Shard:        shardID,
			Data:         studData,
			CurrentIndex: currentIndex,
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			log.Fatalln("Error marshaling JSON: ", err)
			return
		}

		serverIDs := getServerIDsForShard(db, shardID)
		for _, serverID := range serverIDs {
			resp, err := http.Post("http://"+getServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(SERVER_PORT)+"/write", "application/json", bytes.NewBuffer(payloadData))
			if err != nil {
				log.Println("Error writing to Server:", err)
				return
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Println("Error reading response body:", err)
			}

			var respData ServerWriteResponse
			json.Unmarshal(body, &respData)
			resp.Body.Close()

			if respData.CurrentIndex != currentIndex+len(studData) {
				log.Println("Error writing to Server: Invalid Index")
				return
			}
		}

		_, err = db.Exec("UPDATE shardt SET valid_idx = $1 WHERE shard_id = $2;", currentIndex+len(studData), shardID)
		if err != nil {
			log.Fatal(err)
		}

		shardTConfigs[shardID].mutex.Unlock()
	}

	response := WriteResponse{
		Status:  "success",
		Message: fmt.Sprintf("%d Data entries added", len(req.Data)),
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

	var req UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	shardID := getShardIDFromStudID(db, req.StudID)
	shardTConfigs[shardID].mutex.Lock()

	payload := ServerUpdatePayload{
		Shard:  shardID,
		StudID: req.StudID,
		Data:   req.Data,
	}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("Error marshaling JSON: ", err)
		return
	}

	serverIDs := getServerIDsForShard(db, shardID)
	for _, serverID := range serverIDs {
		req, err := http.NewRequest("PUT", "http://"+getServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(SERVER_PORT)+"/update", bytes.NewBuffer(payloadData))
		if err != nil {
			log.Println("Error updating Server:", err)
			return
		}
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Println("Error updating Server:", err)
			return
		}
		resp.Body.Close()
	}
	shardTConfigs[shardID].mutex.Unlock()

	response := UpdateResponse{
		Status:  "success",
		Message: fmt.Sprintf("Data entry for Stud_id: %d updated", req.StudID),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	var req DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	shardID := getShardIDFromStudID(db, req.StudID)
	shardTConfigs[shardID].mutex.Lock()

	payload := ServerDeletePayload{
		Shard:  shardID,
		StudID: req.StudID,
	}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("Error marshaling JSON: ", err)
		return
	}

	serverIDs := getServerIDsForShard(db, shardID)
	for _, serverID := range serverIDs {
		req, err := http.NewRequest("DELETE", "http://"+getServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(SERVER_PORT)+"/delete", bytes.NewBuffer(payloadData))
		if err != nil {
			log.Println("Error deleting from Server:", err)
			return
		}
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Println("Error deleting from Server:", err)
			return
		}
		resp.Body.Close()
	}

	shardTConfigs[shardID].mutex.Unlock()

	response := DeleteResponse{
		Message: fmt.Sprintf("Data entry with Stud_id: %d removed from all replicas", req.StudID),
		Status:  "success",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func main() {
	buildServerInstance()

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	var err error
	db, err = sql.Open("postgres", DB_CONNECTION_STRING)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	shardTConfigs = make(map[string]ShardTConfig)

	serverDown = make(chan int)
	go monitorServers(sigs)

	http.HandleFunc("/init", initHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/add", addServersHandler)
	http.HandleFunc("/rm", removeServersHandler)
	http.HandleFunc("/read", readHandler)
	http.HandleFunc("/write", WriteHandler)
	http.HandleFunc("/update", updateHandler)
	http.HandleFunc("/del", deleteHandler)

	server := &http.Server{Addr: ":5000", Handler: nil}

	go func() {
		<-sigs
		sigs <- os.Interrupt
		server.Shutdown(context.Background())
	}()

	log.Println("Load Balancer running on port 5000")
	err = server.ListenAndServe()
	if err != http.ErrServerClosed {
		log.Fatalln(err)
	} else {
		log.Println("Load Balancer shut down successfully")
	}

	cleanupServers(serverIDs)
	os.Exit(0)
}
