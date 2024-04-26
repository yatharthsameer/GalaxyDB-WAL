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
	"syscall"
	"time"

	_ "github.com/lib/pq"

	galaxy "github.com/yatharthsameer/galaxydb/loadbalancer/internal"
)

var (
	db         *sql.DB
	serverDown chan int
)

func getServerIDs() []int {
	resp, err := http.Get(galaxy.LOADBALANCER_URL + "/serverids")
	if err != nil {
		log.Println("Error getting servers list from loadbalancer:", err)
	}
	defer resp.Body.Close()

	serverIDs := []int{}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("Error reading response body:", err)
	}

	err = json.Unmarshal(body, &serverIDs)
	if err != nil {
		log.Println("Error unmarshaling JSON: ", err)
	}

	return serverIDs
}

func checkHeartbeat(serverID int, serverDown chan<- int) {
	for {
		isPresent := false
		for _, server := range getServerIDs() {
			if server == serverID {
				isPresent = true
				break
			}
		}
		if !isPresent {
			return
		}
		serverIP := galaxy.GetServerIP(fmt.Sprintf("Server%d", serverID))
		if len(serverIP) == 0 {
			log.Println("Server", serverID, " is down!")
			serverDown <- serverID
			return
		}
		resp, err := http.Get("http://" + serverIP + ":" + fmt.Sprint(galaxy.SERVER_PORT) + "/heartbeat")
		if err != nil || resp.StatusCode != http.StatusOK {
			log.Println("Server", serverID, " is down!")
			serverDown <- serverID
			return
		}
		time.Sleep(5 * time.Second)
	}
}

func monitorServers(stopSignal chan os.Signal) {
	for _, server := range getServerIDs() {
		go checkHeartbeat(server, serverDown)
	}

	for {
		select {
		case <-stopSignal:
			stopSignal <- os.Interrupt
			return
		case downServerID := <-serverDown:
			newServerID := galaxy.GetRandomID()

			payload := galaxy.ReplaceServerRequest{
				DownServerID: downServerID,
				NewServerID:  newServerID,
			}

			payloadData, err := json.Marshal(payload)
			if err != nil {
				log.Println("Error marshaling JSON: ", err)
			}

			log.Printf("Restarting Server%d as Server%d\n", downServerID, newServerID)
			_, err = http.Post(galaxy.LOADBALANCER_URL+"/replace_server", "application/json", bytes.NewBuffer(payloadData))
			if err != nil {
				log.Println("Error replacing server: ", err)
				continue
			}

			go checkHeartbeat(newServerID, serverDown)
		}
	}
}

func checkHeartbeatHandler(w http.ResponseWriter, r *http.Request) {
	var serverID int
	err := json.NewDecoder(r.Body).Decode(&serverID)
	if err != nil {
		log.Println("Error decoding JSON: ", err)
	}

	go checkHeartbeat(serverID, serverDown)

	w.WriteHeader(http.StatusOK)
}

func shardServersHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	var req galaxy.ShardServersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	var servers []int
	rows, err := db.Query("SELECT server_id FROM MapT WHERE shard_id = $1;", req.ShardID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error running sql query to get servers for a shard: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var server int
		err := rows.Scan(&server)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error reading sql result: %v", err), http.StatusInternalServerError)
			return
		}
		servers = append(servers, server)
	}

	var primary int
	err = db.QueryRow("SELECT server_id FROM MapT WHERE shard_id = $1 and is_primary = TRUE;", req.ShardID).Scan(&primary)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error running sql query to get a primary server: %v", err), http.StatusInternalServerError)
		return
	}

	response := galaxy.ShardServersResponse{
		ServerIDs: servers,
		Primary:   primary,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

func primaryElectHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not supported", http.StatusMethodNotAllowed)
		return
	}

	var req galaxy.PrimaryElectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Error decoding request: %v", err), http.StatusBadRequest)
		return
	}

	for _, shard := range req.ShardIDs {
		rows, err := db.Query("SELECT server_id FROM MapT WHERE shard_id = $1;", shard)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error getting mapt entry: %v", err), http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		var primaryServer int
		maxWalLength := 0

		for rows.Next() {
			var server int
			err := rows.Scan(&server)
			if err != nil {
				http.Error(w, fmt.Sprintf("Errorscanning rows: %v", err), http.StatusInternalServerError)
				return
			}

			walLength := galaxy.GetServerWalLength(server)
			if walLength >= maxWalLength {
				maxWalLength = walLength
				primaryServer = server
			}
		}

		log.Println("Setting primary server for", shard, "as Server", primaryServer)

		_, err = db.Exec("UPDATE MapT SET is_primary = TRUE WHERE shard_id = $1 AND server_id = $2;", shard, primaryServer)
		if err != nil {
			log.Fatal(err)
		}
	}

	w.WriteHeader(http.StatusOK)
}

func main() {
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

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	serverDown = make(chan int)
	go monitorServers(sigs)

	server := &http.Server{Addr: ":8000", Handler: nil}

	go func() {
		<-sigs
		sigs <- os.Interrupt
		server.Shutdown(context.Background())
	}()

	http.HandleFunc("/check_heartbeat", checkHeartbeatHandler)
	http.HandleFunc("/shard_servers", shardServersHandler)
	http.HandleFunc("/primary_elect", primaryElectHandler)

	log.Println("Shard Manager running on port 8000")
	err = server.ListenAndServe()
	if err != http.ErrServerClosed {
		log.Fatalln(err)
	} else {
		log.Println("Shard Manager shut down successfully")
	}

	galaxy.CleanupServers(getServerIDs())
}
