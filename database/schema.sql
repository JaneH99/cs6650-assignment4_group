-- CS6650 Assignment 3 Schema
-- Run this once against PostgreSQL instance before starting the consumer
-- Note: chatdb database is created automatically by docker-compose

CREATE TABLE IF NOT EXISTS messages (
    message_id  VARCHAR(255) PRIMARY KEY,
    room_id     VARCHAR(255) NOT NULL,
    user_id     VARCHAR(255) NOT NULL,
    timestamp   VARCHAR(64)  NOT NULL,
    content     TEXT
);

-- Supports: get messages for a room in time range (Query 1)
CREATE INDEX IF NOT EXISTS idx_messages_room_timestamp
    ON messages (room_id, timestamp);

-- Supports: user message history (Query 2) + rooms per user (Query 4)
CREATE INDEX IF NOT EXISTS idx_messages_user_room_timestamp
    ON messages (user_id, room_id, timestamp);

-- Supports: count active users in time window (Query 3)
CREATE INDEX IF NOT EXISTS idx_messages_timestamp
    ON messages (timestamp);
