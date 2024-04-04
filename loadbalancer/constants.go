package main

const (
	SERVER_DOCKER_IMAGE_NAME = "galaxydb-server"
	DOCKER_NETWORK_NAME      = "galaxydb-network"
	SERVER_PORT              = 5000
	DB_FILENAME              = "galaxy-lb.db"
	INIT_DB                  = `CREATE TABLE IF NOT EXISTS shardt (
									stud_id_low INT PRIMARY KEY,
									shard_id TEXT,
									shard_size INT,
									valid_idx INT
								);
								CREATE TABLE IF NOT EXISTS mapt (
									shard_id TEXT,
									server_id INT
								);`
)
