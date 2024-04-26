package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	_ "github.com/lib/pq"

	galaxy "github.com/yatharthsameer/galaxydb/loadbalancer/internal"
	"github.com/yatharthsameer/galaxydb/loadbalancer/internal/consistenthashmap"
)

var (
	schemaConfig  galaxy.SchemaConfig
	shardTConfigs map[string]galaxy.ShardTConfig
	serverIDs     []int
	db            *sql.DB
	serverDown    chan int
)

func initHandler(w http.ResponseWriter, r *http.Request) {
	var req galaxy.InitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	schemaConfig = req.Schema

	for rawServerName, shardIDs := range req.Servers {
		serverID := galaxy.GetServerID(rawServerName)

		for _, shardID := range shardIDs {
			_, err := db.Exec("INSERT INTO mapt (shard_id, server_id) VALUES ($1, $2);", shardID, serverID)
			if err != nil {
				log.Fatal(err)
			}
		}

		serverIDs = append(serverIDs, serverID)

		galaxy.SpawnNewServerInstance(fmt.Sprintf("Server%d", serverID), serverID)
		galaxy.ConfigNewServerInstance(serverID, shardIDs)

		_, err := http.Post(galaxy.SHARD_MANAGER_URL+"/check_heartbeat", "application/json", bytes.NewBuffer([]byte(fmt.Sprint(serverID))))
		if err != nil {
			log.Println("Error checking heartbeat:", err)
		}
	}

	var shardIDs []string

	for _, shard := range req.Shards {
		_, err := db.Exec("INSERT INTO shardt (stud_id_low, shard_id, shard_size, valid_idx) VALUES ($1, $2, $3, $4);", shard.StudIDLow, shard.ShardID, shard.ShardSize, 0)
		if err != nil {
			log.Fatal(err)
		}

		config := shardTConfigs[shard.ShardID]
		config.CHM = &consistenthashmap.ConsistentHashMap{}
		config.Mutex = &sync.Mutex{}
		shardTConfigs[shard.ShardID] = config

		shardTConfigs[shard.ShardID].CHM.Init()
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
			shardTConfigs[shard.ShardID].CHM.AddServer(serverID)
		}

		shardIDs = append(shardIDs, shard.ShardID)
	}

	payload := galaxy.PrimaryElectRequest{
		ShardIDs: shardIDs,
	}

	payloadData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("Error marshaling JSON: ", err)
		return
	}

	_, err = http.Post(galaxy.SHARD_MANAGER_URL+"/primary_elect", "application/json", bytes.NewBuffer(payloadData))
	if err != nil {
		log.Println("Error electing primary:", err)
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

	shards := []galaxy.Shard{}
	rows, err := db.Query("SELECT stud_id_low, shard_id, shard_size FROM shardt;")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var shard galaxy.Shard
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
	var req galaxy.AddRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.Servers) < req.N {
		resp := galaxy.AddResponseFailed{
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
		serverID := galaxy.GetServerID(rawServerName)
		serverIDsAdded = append(serverIDsAdded, serverID)

		for _, shardID := range shardIDs {
			_, err := db.Exec("INSERT INTO mapt (shard_id, server_id) VALUES ($1, $2);", shardID, serverID)
			if err != nil {
				log.Fatal(err)
			}
		}

		serverIDs = append(serverIDs, serverID)

		galaxy.SpawnNewServerInstance(fmt.Sprintf("Server%d", serverID), serverID)
		galaxy.ConfigNewServerInstance(serverID, shardIDs)

		_, err := http.Post(galaxy.SHARD_MANAGER_URL+"/check_heartbeat", "application/json", bytes.NewBuffer([]byte(fmt.Sprint(serverID))))
		if err != nil {
			log.Println("Error checking heartbeat:", err)
		}
	}

	var shardIDs []string

	for _, shard := range req.NewShards {
		_, err := db.Exec("INSERT INTO shardt (stud_id_low, shard_id, shard_size, valid_idx) VALUES ($1, $2, $3, $4);", shard.StudIDLow, shard.ShardID, shard.ShardSize, 0)
		if err != nil {
			log.Fatal(err)
		}

		config := shardTConfigs[shard.ShardID]
		config.CHM = &consistenthashmap.ConsistentHashMap{}
		config.Mutex = &sync.Mutex{}
		shardTConfigs[shard.ShardID] = config

		shardTConfigs[shard.ShardID].CHM.Init()
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
			shardTConfigs[shard.ShardID].CHM.AddServer(serverID)
		}

		shardIDs = append(shardIDs, shard.ShardID)
	}

	payload := galaxy.PrimaryElectRequest{
		ShardIDs: shardIDs,
	}

	payloadData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("Error marshaling JSON: ", err)
		return
	}

	_, err = http.Post(galaxy.SHARD_MANAGER_URL+"/primary_elect", "application/json", bytes.NewBuffer(payloadData))
	if err != nil {
		log.Println("Error electing primary:", err)
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

	response := galaxy.AddResponseSuccess{
		N:       len(serverIDs),
		Message: addServerMessage,
		Status:  "successful",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func removeServersHandler(w http.ResponseWriter, r *http.Request) {
	var req galaxy.RemoveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	if len(req.Servers) > req.N {
		resp := galaxy.RemoveResponseFailed{
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
		serverIDsRemoved = append(serverIDsRemoved, galaxy.GetServerID(serverName))
	}

	additionalRemovalsNeeded := req.N - len(serverIDsRemoved)
	for additionalRemovalsNeeded > 0 {
		if serverID := galaxy.ChooseRandomServerForRemoval(serverIDs, serverIDsRemoved); serverID != -1 {
			serverIDsRemoved = append(serverIDsRemoved, serverID)
			additionalRemovalsNeeded -= 1
		}
	}
	primaryShardList := []string{}
	for _, serverIDRemoved := range serverIDsRemoved {
		shardIDsRemoved := []string{}
		rows, err := db.Query("SELECT shard_id, is_primary FROM mapt WHERE server_id = $1;", serverIDRemoved)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()

		for rows.Next() {
			var shardID string
			isPrimary := false
			err = rows.Scan(&shardID, &isPrimary)
			if err != nil {
				log.Fatal(err)
			}
			shardIDsRemoved = append(shardIDsRemoved, shardID)
			if isPrimary {
				primaryShardList = append(primaryShardList, shardID)
			}
		}

		for _, shardIDRemoved := range shardIDsRemoved {
			shardTConfigs[shardIDRemoved].CHM.RemoveServer(serverIDRemoved)
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
		galaxy.RemoveServerInstance(serverNameRemoved)

		serverNamesRemoved = append(serverNamesRemoved, serverNameRemoved)
	}

	payload := galaxy.PrimaryElectRequest{
		ShardIDs: primaryShardList,
	}

	payloadData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("Error marshaling JSON: ", err)
		return
	}

	_, err = http.Post(galaxy.SHARD_MANAGER_URL+"/primary_elect", "application/json", bytes.NewBuffer(payloadData))
	if err != nil {
		log.Println("Error electing primary:", err)
	}

	response := galaxy.RemoveResponseSuccess{
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

	var req galaxy.ReadRequest
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

	var studData []galaxy.StudT
	for _, shardIDQueried := range shardIDsQueried {
		payload := galaxy.ServerReadPayload{
			Shard:  shardIDQueried,
			StudID: req.StudID,
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			log.Fatalln("Error marshaling JSON: ", err)
			return
		}

		serverID := shardTConfigs[shardIDQueried].CHM.GetServerForRequest(galaxy.GetRandomID())

		resp, err := http.Post("http://"+galaxy.GetServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(galaxy.SERVER_PORT)+"/read", "application/json", bytes.NewBuffer(payloadData))
		if err != nil {
			log.Println("Error reading from Server:", err)
			return
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Println("Error reading response body:", err)
		}

		var respData galaxy.ServerReadResponse
		json.Unmarshal(body, &respData)
		resp.Body.Close()

		studData = append(studData, respData.Data...)
	}

	response := galaxy.ReadResponse{
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

	var req galaxy.WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	studDataToWrite := map[string][]galaxy.StudT{}
	for _, studData := range req.Data {
		shardID := galaxy.GetShardIDFromStudID(db, studData.StudID)
		studDataToWrite[shardID] = append(studDataToWrite[shardID], studData)
	}

	for shardID, studData := range studDataToWrite {
		shardTConfigs[shardID].Mutex.Lock()

		payload := galaxy.ServerWritePayload{
			Shard: shardID,
			Data:  studData,
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error marshaling JSON: %v", err), http.StatusInternalServerError)
			return
		}

		serverIDs := galaxy.GetServerIDsForShard(db, shardID)
		for _, serverID := range serverIDs {
			resp, err := http.Post("http://"+galaxy.GetServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(galaxy.SERVER_PORT)+"/write", "application/json", bytes.NewBuffer(payloadData))
			if err != nil {
				http.Error(w, fmt.Sprintf("Error writing %s record to Server%d: %v", shardID, serverID, err), http.StatusInternalServerError)
				return
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Error reading response body from Server%d: %v\n", serverID, err)
			}

			var respData galaxy.ServerWriteResponse
			json.Unmarshal(body, &respData)
			resp.Body.Close()
		}

		shardTConfigs[shardID].Mutex.Unlock()
	}

	response := galaxy.WriteResponse{
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

	var req galaxy.UpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	shardID := galaxy.GetShardIDFromStudID(db, req.StudID)
	shardTConfigs[shardID].Mutex.Lock()

	payload := galaxy.ServerUpdatePayload{
		Shard:  shardID,
		StudID: req.StudID,
		Data:   req.Data,
	}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("Error marshaling JSON: ", err)
		return
	}

	serverIDs := galaxy.GetServerIDsForShard(db, shardID)
	for _, serverID := range serverIDs {
		req, err := http.NewRequest("PUT", "http://"+galaxy.GetServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(galaxy.SERVER_PORT)+"/update", bytes.NewBuffer(payloadData))
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
	shardTConfigs[shardID].Mutex.Unlock()

	response := galaxy.UpdateResponse{
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

	var req galaxy.DeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	shardID := galaxy.GetShardIDFromStudID(db, req.StudID)
	shardTConfigs[shardID].Mutex.Lock()

	payload := galaxy.ServerDeletePayload{
		Shard:  shardID,
		StudID: req.StudID,
	}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("Error marshaling JSON: ", err)
		return
	}

	serverIDs := galaxy.GetServerIDsForShard(db, shardID)
	for _, serverID := range serverIDs {
		req, err := http.NewRequest("DELETE", "http://"+galaxy.GetServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(galaxy.SERVER_PORT)+"/delete", bytes.NewBuffer(payloadData))
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

	shardTConfigs[shardID].Mutex.Unlock()

	response := galaxy.DeleteResponse{
		Message: fmt.Sprintf("Data entry with Stud_id: %d removed from all replicas", req.StudID),
		Status:  "success",
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func serverIDsHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(serverIDs)
}

func replaceServerHandler(w http.ResponseWriter, r *http.Request) {
	var req galaxy.ReplaceServerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	serverIDs = galaxy.ReplaceServerInstance(db, req.DownServerID, req.NewServerID, serverIDs, shardTConfigs)

	w.WriteHeader(http.StatusOK)
}

func main() {
	galaxy.BuildServerInstance()

	var err error
	db, err = sql.Open("postgres", galaxy.DB_CONNECTION_STRING)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	shardTConfigs = make(map[string]galaxy.ShardTConfig)

	http.HandleFunc("/init", initHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/add", addServersHandler)
	http.HandleFunc("/rm", removeServersHandler)
	http.HandleFunc("/read", readHandler)
	http.HandleFunc("/write", WriteHandler)
	http.HandleFunc("/update", updateHandler)
	http.HandleFunc("/del", deleteHandler)
	http.HandleFunc("/serverids", serverIDsHandler)
	http.HandleFunc("/replace_server", replaceServerHandler)

	server := &http.Server{Addr: ":5000", Handler: nil}

	log.Println("Load Balancer running on port 5000")
	err = server.ListenAndServe()
	if err != http.ErrServerClosed {
		log.Fatalln(err)
	} else {
		log.Println("Load Balancer shut down successfully")
	}
}
