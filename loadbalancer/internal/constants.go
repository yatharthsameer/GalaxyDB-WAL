package galaxydb

const (
	SERVER_DOCKER_IMAGE_NAME = "galaxydb-server"
	DOCKER_NETWORK_NAME      = "galaxydb-network"
	SERVER_PORT              = 5000
	DB_CONNECTION_STRING     = "host=galaxydb-metadata user=postgres password=galaxydb dbname=postgres port=5432 sslmode=disable"
)
