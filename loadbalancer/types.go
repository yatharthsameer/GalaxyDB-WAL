package main

import (
	"sync"

	"github.com/yatharthsameer/galaxydb/loadbalancer/internal/consistenthashmap"
)

type ShardTConfig struct {
	chm   *consistenthashmap.ConsistentHashMap
	mutex *sync.Mutex
}

type Shard struct {
	StudIDLow int    `json:"Stud_id_low"`
	ShardID   string `json:"Shard_id"`
	ShardSize int    `json:"Shard_size"`
}

type SchemaConfig struct {
	Columns []string `json:"columns"`
	Dtypes  []string `json:"dtypes"`
}

type InitRequest struct {
	N       int                 `json:"N"`
	Schema  SchemaConfig        `json:"schema"`
	Shards  []Shard             `json:"shards"`
	Servers map[string][]string `json:"servers"`
}

type AddRequest struct {
	N         int                 `json:"n"`
	NewShards []Shard             `json:"new_shards"`
	Servers   map[string][]string `json:"servers"`
}

type AddResponseSuccess struct {
	N       int    `json:"N"`
	Message string `json:"message"`
	Status  string `json:"status"`
}

type AddResponseFailed struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

type RemoveRequest struct {
	N       int      `json:"n"`
	Servers []string `json:"servers"`
}

type RemoveResponseSuccess struct {
	Message map[string]interface{} `json:"message"`
	Status  string                 `json:"status"`
}

type RemoveResponseFailed struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

type ServerConfigPayload struct {
	Schema SchemaConfig `json:"schema"`
	Shards []string     `json:"shards"`
}

type StudT struct {
	StudID    int    `json:"Stud_id"`
	StudName  string `json:"Stud_name"`
	StudMarks int    `json:"Stud_marks"`
}

type ReadRequest struct {
	StudID struct {
		Low  int `json:"low"`
		High int `json:"high"`
	} `json:"Stud_id"`
}

type ReadResponse struct {
	ShardsQueried []string `json:"shards_queried"`
	Data          []StudT  `json:"data"`
	Status        string   `json:"status"`
}

type ServerReadPayload struct {
	Shard  string `json:"shard"`
	StudID struct {
		Low  int `json:"low"`
		High int `json:"high"`
	} `json:"Stud_id"`
}

type ServerReadResponse struct {
	Status string  `json:"status"`
	Data   []StudT `json:"data"`
}

type WriteRequest struct {
	Data []StudT `json:"data"`
}

type WriteResponse struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

type ServerWritePayload struct {
	Shard        string  `json:"shard"`
	CurrentIndex int     `json:"curr_idx"`
	Data         []StudT `json:"data"`
}

type ServerWriteResponse struct {
	Status       string `json:"status"`
	Message      string `json:"message"`
	CurrentIndex int    `json:"current_idx"`
}

type UpdateRequest struct {
	StudID int   `json:"Stud_id"`
	Data   StudT `json:"data"`
}

type UpdateResponse struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

type ServerUpdatePayload struct {
	Shard  string `json:"shard"`
	StudID int    `json:"Stud_id"`
	Data   StudT  `json:"data"`
}

type DeleteRequest struct {
	StudID int `json:"Stud_id"`
}

type DeleteResponse struct {
	Message string `json:"message"`
	Status  string `json:"status"`
}

type ServerDeletePayload struct {
	Shard  string `json:"shard"`
	StudID int    `json:"Stud_id"`
}

type ServerCopyPayload struct {
	Shards []string `json:"shards"`
}

type ServerCopyResponse map[string][]StudT
