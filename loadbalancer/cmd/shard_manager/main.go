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
	resp, err := http.Get("http://localhost:5000/serverids")
	if err != nil {
		log.Fatal(err)
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
			fmt.Printf("Server%d is down!\n", serverID)
			serverDown <- serverID
			return
		}
		resp, err := http.Get("http://" + serverIP + ":" + fmt.Sprint(galaxy.SERVER_PORT) + "/heartbeat")
		if err != nil || resp.StatusCode != http.StatusOK {
			fmt.Printf("Server%d is down!\n", serverID)
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

			_, err = http.Post("http://localhost:5000", "application/json", bytes.NewBuffer(payloadData))
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

	http.HandleFunc("check_heartbeat", checkHeartbeatHandler)

	log.Println("Shard Manager running on port 8000")
	err = server.ListenAndServe()
	if err != http.ErrServerClosed {
		log.Fatalln(err)
	} else {
		log.Println("Shard Manager shut down successfully")
	}

	galaxy.CleanupServers(getServerIDs())
}
