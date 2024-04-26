package galaxydb

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
)

func GetSchemaConfig() SchemaConfig {
	var schemaConfig SchemaConfig
	schemaConfig.Columns = []string{"Stud_id", "Stud_name", "Stud_marks"}
	schemaConfig.Dtypes = []string{"Number", "String", "Number"}

	return schemaConfig
}

func GetRandomID() int {
	return rand.Intn(900000) + 100000
}

func GetServerID(rawServerName string) int {
	rawServerID := rawServerName[len("Server"):]
	serverID, err := strconv.Atoi(rawServerID)
	if err != nil {
		return GetRandomID()
	}
	return serverID
}

func BuildServerInstance() error {
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
		return fmt.Errorf("failed to build server image: %v", err)
	} else {
		log.Println("Server image built successfully")
		return nil
	}
}

func SpawnNewServerInstance(hostname string, id int) error {
	cmd := exec.Command("sudo", "docker", "run", "--rm", "-d", "--name", hostname, "--network", DOCKER_NETWORK_NAME, "-e", fmt.Sprintf("id=%d", id), fmt.Sprintf("%s:latest", SERVER_DOCKER_IMAGE_NAME))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to start new server instance: %v, stderr: %s", err, stderr.String())
	}
	return nil
}

func GetServerIP(hostname string) string {
	cmd := exec.Command("sudo", "docker", "inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", hostname)
	output, err := cmd.Output()
	if err != nil {
		log.Printf("failed to get IP for server '%s': %v\n", hostname, err)
		return ""
	}

	return strings.TrimSpace(string(output))
}

func ConfigNewServerInstance(serverID int, shards []string) error {
	payload := ServerConfigPayload{
		Schema: GetSchemaConfig(),
		Shards: shards,
	}
	payloadData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling JSON: %v", err)
	}

	resp, err := http.Post("http://"+GetServerIP(fmt.Sprintf("Server%d", serverID))+":"+fmt.Sprint(SERVER_PORT)+"/config", "application/json", bytes.NewBuffer(payloadData))
	if err != nil {
		return fmt.Errorf("error configuring Server: %v", err)
	}
	defer resp.Body.Close()
	return nil
}

func RemoveServerInstance(hostname string) error {
	cmd := exec.Command("sudo", "docker", "stop", hostname)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to stop server instance '%s': %v", hostname, err)
	}
	return nil
}

func CleanupServers(serverIDs []int) {
	log.Println("Cleaning up server instances...")

	for _, server := range serverIDs {
		stopCmd := exec.Command("sudo", "docker", "stop", fmt.Sprintf("Server%d", server))
		if err := stopCmd.Run(); err != nil {
			log.Printf("Failed to stop server '%d': %v\n", server, err)
		}
	}
}

func ChooseRandomServerForRemoval(serverIDs []int, serverIDsRemoved []int) int {
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

func GetShardIDFromStudID(db *sql.DB, studID int) (string, error) {
	row, err := db.Query("SELECT shard_id FROM shardt WHERE $1 BETWEEN stud_id_low AND stud_id_low+shard_size", studID)
	if err != nil {
		return "", fmt.Errorf("error querying shardt: %v", err)
	}
	defer row.Close()

	var shardID string
	for row.Next() {
		err := row.Scan(&shardID)
		if err != nil {
			return "", fmt.Errorf("error scanning row: %v", err)
		}
	}

	return shardID, nil
}

func GetValidIDx(db *sql.DB, shardID string) (int, error) {
	row, err := db.Query("SELECT valid_idx FROM shardt WHERE shard_id=$1", shardID)
	if err != nil {
		return -1, fmt.Errorf("error querying shardt: %v", err)
	}
	defer row.Close()

	var validIDx int
	for row.Next() {
		err := row.Scan(&validIDx)
		if err != nil {
			return -1, fmt.Errorf("error scanning row: %v", err)
		}
	}

	return validIDx, nil
}

func GetServerIDsForShard(db *sql.DB, shardID string) ([]int, error) {
	row, err := db.Query("SELECT server_id FROM mapt WHERE shard_id=$1", shardID)
	if err != nil {
		return nil, fmt.Errorf("error querying mapt: %v", err)
	}
	defer row.Close()

	var serverID int
	serverIDs := []int{}
	for row.Next() {
		err := row.Scan(&serverID)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}
		serverIDs = append(serverIDs, serverID)
	}

	return serverIDs, nil
}

func ReplaceServerInstance(db *sql.DB, downServerID int, newServerID int, serverIDs []int, shardTConfigs map[string]ShardTConfig) ([]int, error) {
	err := SpawnNewServerInstance(fmt.Sprintf("Server%d", newServerID), newServerID)
	if err != nil {
		return nil, fmt.Errorf("error spawning new server: %v", err)
	}

	rows, err := db.Query("SELECT shard_id FROM mapt WHERE server_id=$1", downServerID)
	if err != nil {
		return nil, fmt.Errorf("error querying mapt: %v", err)
	}

	shardIDs := []string{}
	for rows.Next() {
		var shardID string
		err := rows.Scan(&shardID)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}
		shardIDs = append(shardIDs, shardID)
	}
	rows.Close()

	err = ConfigNewServerInstance(newServerID, shardIDs)
	if err != nil {
		return nil, fmt.Errorf("error configuring new server: %v", err)
	}

	primaryShardList := []string{}

	for _, shardID := range shardIDs {
		shardTConfigs[shardID].Mutex.Lock()
		shardTConfigs[shardID].CHM.RemoveServer(downServerID)

		payload := ServerCopyPayload{
			Shards: []string{shardID},
		}
		payloadData, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("error marshaling JSON: %v", err)
		}

		existingServerID := shardTConfigs[shardID].CHM.GetServerForRequest(GetRandomID())

		req, err := http.NewRequest("GET", "http://"+GetServerIP(fmt.Sprintf("Server%d", existingServerID))+":"+fmt.Sprint(SERVER_PORT)+"/copy", bytes.NewBuffer(payloadData))
		if err != nil {
			return nil, fmt.Errorf("error creating request: %v", err)
		}
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("error copying from server: %v", err)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body: %v", err)
		}

		var respData ServerCopyResponse
		json.Unmarshal(body, &respData)
		resp.Body.Close()

		shardData := respData[shardID]

		payloadWrite := ServerWritePayload{
			Shard: shardID,
			Data:  shardData,
		}
		payloadData, err = json.Marshal(payloadWrite)
		if err != nil {
			return nil, fmt.Errorf("error marshaling JSON: %v", err)
		}

		_, err = http.Post("http://"+GetServerIP(fmt.Sprintf("Server%d", newServerID))+":"+fmt.Sprint(SERVER_PORT)+"/write", "application/json", bytes.NewBuffer(payloadData))
		if err != nil {
			return nil, fmt.Errorf("error writing to new server: %v", err)
		}

		shardTConfigs[shardID].CHM.AddServer(newServerID)
		shardTConfigs[shardID].Mutex.Unlock()

		row := db.QueryRow("SELECT is_primary FROM mapt WHERE shard_id=$1 AND server_id=$2", shardID, downServerID)
		isPrimary := false
		err = row.Scan(&isPrimary)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}
		if isPrimary {
			primaryShardList = append(primaryShardList, shardID)
		}
	}

	_, err = db.Exec("UPDATE mapt SET server_id=$1, is_primary=FALSE WHERE server_id=$2", newServerID, downServerID)
	if err != nil {
		return nil, fmt.Errorf("error updating mapt: %v", err)
	}

	if len(primaryShardList) != 0 {
		payload := PrimaryElectRequest{
			ShardIDs: primaryShardList,
		}

		payloadData, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("error marshaling JSON: %v", err)
		}

		_, err = http.Post(SHARD_MANAGER_URL+"/primary_elect", "application/json", bytes.NewBuffer(payloadData))
		if err != nil {
			return nil, fmt.Errorf("error electing primary: %v", err)
		}
	}

	newServerIDs := []int{}
	for _, serverID := range serverIDs {
		if serverID != downServerID {
			newServerIDs = append(newServerIDs, serverID)
		}
	}
	newServerIDs = append(newServerIDs, newServerID)

	return newServerIDs, nil
}

func GetServerWalLength(serverID int) (int, error) {
	resp, err := http.Get("http://" + GetServerIP(fmt.Sprintf("Server%d", serverID)) + ":" + fmt.Sprint(SERVER_PORT) + "/wal_length")
	if err != nil {
		return -1, fmt.Errorf("error getting WAL length from Server: %v", err)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return -1, fmt.Errorf("error reading response body: %v", err)
	}

	var walLength int
	json.Unmarshal(body, &walLength)
	resp.Body.Close()

	return walLength, nil
}
