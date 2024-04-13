package main

import "time"

type ConfigPayload struct {
	Schema schema   `json:"schema"`
	Shards []string `json:"shards"`
}

type schema struct {
	Columns []string `json:"columns"`
	Dtypes  []string `json:"dtypes"`
}

type CopyRequest struct {
	Shards []string `json:"shards"`
}

type ShardData struct {
	StudentID    int    `json:"Stud_id"`
	StudentName  string `json:"Stud_name"`
	StudentMarks int    `json:"Stud_marks"`
}

type WriteRequest struct {
	Shard     string      `json:"shard"`
	CurrIndex int         `json:"curr_idx"`
	Data      []ShardData `json:"data"`
}

type WriteResponse struct {
	Message    string `json:"message"`
	CurrentIdx int    `json:"current_idx"`
	Status     string `json:"status"`
}

type ReadRequest struct {
	Shard  string `json:"shard"`
	StudID struct {
		Low  int `json:"low"`
		High int `json:"high"`
	} `json:"Stud_id"`
}

type ReadResponse struct {
	Data   []ShardData `json:"data"`
	Status string      `json:"status"`
}

type UpdateRequest struct {
	Shard  string    `json:"shard"`
	StudID int       `json:"Stud_id"`
	Data   ShardData `json:"data"`
}

type DeleteRequest struct {
	Shard  string `json:"shard"`
	StudID int    `json:"Stud_id"`
}

type WALRecord struct {
	Timestamp time.Time   `json:"timestamp"`
	Shard     string      `json:"shard"`
	Data      []ShardData `json:"data"`
}

type IsPRimaryRequest struct {
	ShardID  string `json:"shard_id"`
	ServerID int    `json:"server_id"`
}
