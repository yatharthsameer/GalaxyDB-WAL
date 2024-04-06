package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func getRandomID() int {
	return rand.Intn(900000) + 100000
}

func getServerID(rawServerName string) int {
	rawServerID := rawServerName[len("Server"):]
	serverID, err := strconv.Atoi(rawServerID)
	if err != nil {
		return getRandomID()
	}
	return serverID
}

func buildServerInstance() {
	env := os.Getenv("GO_ENV")
	var serverPath string
	if env == "production" {
		serverPath = "/server"
	} else {
		serverPath = "../server"
	}

	cmd := exec.Command("sudo", "docker", "build", "--tag", SERVER_DOCKER_IMAGE_NAME, serverPath)
	err := cmd.Run()
	if err != nil {
		log.Fatalln("Failed to build server image: ", err)
	} else {
		log.Println("Server image built successfully")
	}
}

func spawnNewServerInstance(hostname string, id int) {
	cmd := exec.Command("sudo", "docker", "run", "--rm", "-d", "--name", hostname, "--network", DOCKER_NETWORK_NAME, "-e", fmt.Sprintf("id=%d", id), fmt.Sprintf("%s:latest", SERVER_DOCKER_IMAGE_NAME))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to start new server instance: %v, stderr: %s", err, stderr.String())
	}
}

func getServerIP(hostname string) string {
	cmd := exec.Command("sudo", "docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", hostname)
	output, err := cmd.Output()
	if err != nil {
		log.Println("Error running docker inspect: ", err)
		return ""
	}

	return strings.TrimSpace(string(output))
}

func configNewServerInstance(serverID int, shards []string, schema SchemaConfig) {
	payload := ServerConfigPayload{
		Schema: schema,
		Shards: shards,
	}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		log.Fatalln("Error marshaling JSON: ", err)
		return
	}

	resp, err := http.Post("http://"+getServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(SERVER_PORT)+"/config", "application/json", bytes.NewBuffer(payloadData))
	if err != nil {
		log.Println("Error configuring Server:", err)
		return
	}
	defer resp.Body.Close()
}

func removeServerInstance(hostname string) {
	cmd := exec.Command("sudo", "docker", "stop", hostname)
	err := cmd.Run()
	if err != nil {
		log.Fatalf("Failed to stop server instance '%s': %v\n", hostname, err)
		return
	}
}

func cleanupServers(serverIDs []int) {
	log.Println("Cleaning up server instances...")

	for _, server := range serverIDs {
		stopCmd := exec.Command("sudo", "docker", "stop", fmt.Sprintf("Server%d", server))
		if err := stopCmd.Run(); err != nil {
			log.Printf("Failed to stop server '%d': %v\n", server, err)
		}
	}
}

func chooseRandomServerForRemoval(serverIDs []int, serverIDsRemoved []int) int {
	if len(serverIDs)-len(serverIDsRemoved) <= 0 {
		return -1
	}

	serverIDsAvailable := []int{}
	for _, serverID := range serverIDs {
		isPresent := false
		for _, serverIDRemoved := range serverIDsRemoved {
			if serverIDRemoved == serverID {
				isPresent = true
				break
			}
		}
		if !isPresent {
			serverIDsAvailable = append(serverIDsAvailable, serverID)
		}
	}

	index := rand.Intn(len(serverIDsAvailable))
	return serverIDsAvailable[index]
}

func getShardIDFromStudID(db *sql.DB, studID int) string {
	row, err := db.Query("SELECT shard_id FROM shardt WHERE $1 BETWEEN stud_id_low AND stud_id_low+shard_size", studID)
	if err != nil {
		log.Fatal(err)
	}
	defer row.Close()

	var shardID string
	for row.Next() {
		err := row.Scan(&shardID)
		if err != nil {
			log.Fatal(err)
		}
	}

	return shardID
}

func getValidIDx(db *sql.DB, shardID string) int {
	row, err := db.Query("SELECT valid_idx FROM shardt WHERE shard_id=$1", shardID)
	if err != nil {
		log.Fatal(err)
	}
	defer row.Close()

	var validIDx int
	for row.Next() {
		err := row.Scan(&validIDx)
		if err != nil {
			log.Fatal(err)
		}
	}

	return validIDx
}

func getServerIDsForShard(db *sql.DB, shardID string) []int {
	row, err := db.Query("SELECT server_id FROM mapt WHERE shard_id=$1", shardID)
	if err != nil {
		log.Fatal(err)
	}
	defer row.Close()

	var serverID int
	serverIDs := []int{}
	for row.Next() {
		err := row.Scan(&serverID)
		if err != nil {
			log.Fatal(err)
		}
		serverIDs = append(serverIDs, serverID)
	}

	return serverIDs
}

func checkHeartbeat(serverID int, serverDown chan<- int) {
	for {
		isPresent := false
		for _, server := range serverIDs {
			if server == serverID {
				isPresent = true
				break
			}
		}
		if !isPresent {
			return
		}
		serverIP := getServerIP(fmt.Sprintf("Server%d", serverID))
		if len(serverIP) == 0 {
			fmt.Printf("Server%d is down!\n", serverID)
			serverDown <- serverID
			return
		}
		resp, err := http.Get("http://" + serverIP + ":" + fmt.Sprint(SERVER_PORT) + "/heartbeat")
		if err != nil || resp.StatusCode != http.StatusOK {
			fmt.Printf("Server%d is down!\n", serverID)
			serverDown <- serverID
			return
		}
		time.Sleep(5 * time.Second)
	}
}

func replaceServerInstance(downServerID int) {
	newServerID := getRandomID()

	fmt.Printf("Restarting Server%d as Server%d\n", downServerID, newServerID)
	spawnNewServerInstance(fmt.Sprintf("Server%d", newServerID), newServerID)

	rows, err := db.Query("SELECT shard_id FROM mapt WHERE server_id=$1", downServerID)
	if err != nil {
		log.Fatal(err)
	}

	shardIDs := []string{}
	for rows.Next() {
		var shardID string
		err := rows.Scan(&shardID)
		if err != nil {
			log.Fatal(err)
		}
		shardIDs = append(shardIDs, shardID)
	}
	rows.Close()

	configNewServerInstance(newServerID, shardIDs, schemaConfig)

	for _, shardID := range shardIDs {
		shardTConfigs[shardID].mutex.Lock()
		shardTConfigs[shardID].chm.RemoveServer(downServerID)

		payload := ServerCopyPayload{
			Shards: []string{shardID},
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			log.Println("Error marshaling JSON: ", err)
		}

		existingServerID := shardTConfigs[shardID].chm.GetServerForRequest(getRandomID())

		req, err := http.NewRequest("GET", "http://"+getServerIP(fmt.Sprintf("Server%d", existingServerID))+":"+fmt.Sprint(SERVER_PORT)+"/copy", bytes.NewBuffer(payloadData))
		if err != nil {
			log.Println("Error copying from Server:", err)
		}
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			log.Println("Error copying from Server:", err)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Println("Error reading response body:", err)
		}

		var respData ServerCopyResponse
		json.Unmarshal(body, &respData)
		resp.Body.Close()

		shardData := respData[shardID]

		valid_idx := getValidIDx(db, shardID)
		curr_idx := valid_idx - len(shardData)

		payloadWrite := ServerWritePayload{
			Shard:        shardID,
			CurrentIndex: curr_idx,
			Data:         shardData,
		}
		payloadData, err = json.Marshal(payloadWrite)
		if err != nil {
			log.Println("Error marshaling JSON: ", err)
		}

		resp, err = http.Post("http://"+getServerIP(fmt.Sprintf("Server%d", newServerID))+":"+fmt.Sprint(SERVER_PORT)+"/write", "application/json", bytes.NewBuffer(payloadData))
		if err != nil {
			log.Println("Error writing to Server:", err)
		}

		body, err = io.ReadAll(resp.Body)
		if err != nil {
			log.Println("Error reading response body:", err)
		}

		var respDataWrite ServerWriteResponse
		json.Unmarshal(body, &respDataWrite)
		resp.Body.Close()

		if respDataWrite.CurrentIndex != valid_idx {
			log.Println("Error writing to Server: Invalid Index")
		}

		shardTConfigs[shardID].chm.AddServer(newServerID)
		shardTConfigs[shardID].mutex.Unlock()
	}

	_, err = db.Exec("UPDATE mapt SET server_id=$1 WHERE server_id=$2", newServerID, downServerID)
	if err != nil {
		log.Println("Error updating mapt: ", err)
	}

	newServerIDs := []int{}
	for _, serverID := range serverIDs {
		if serverID != downServerID {
			newServerIDs = append(newServerIDs, serverID)
		}
	}
	newServerIDs = append(newServerIDs, newServerID)
	serverIDs = newServerIDs

	go checkHeartbeat(newServerID, serverDown)
}

func monitorServers(stopSignal chan os.Signal) {
	for _, server := range serverIDs {
		go checkHeartbeat(server, serverDown)
	}

	for {
		select {
		case <-stopSignal:
			stopSignal <- os.Interrupt
			return
		case downServerID := <-serverDown:
			replaceServerInstance(downServerID)
		}
	}
}
