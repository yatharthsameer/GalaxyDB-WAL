CREATE TABLE IF NOT EXISTS shardt (
    stud_id_low INT PRIMARY KEY,
    shard_id TEXT,
    shard_size INT,
    valid_idx INT
);


CREATE TABLE IF NOT EXISTS mapt (
    shard_id TEXT,
    server_id INT,
    is_primary BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (shard_id, server_id)
);