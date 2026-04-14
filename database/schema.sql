-- CS6650 Assignment 3 Schema
-- Run this once against PostgreSQL instance before starting the consumer

CREATE DATABASE chatdb;

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

/**
  Materialized Views for analytics queries
  Pre-compute expensive GROUP BY aggregations.
  Unique indexes are required for REFRESH CONCURRENTLY (not block reads)
 */
-- MV 1: Per-user message count (backs Analytics 2: getTopUsers)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_users AS
SELECT user_id, COUNT(*) AS message_count
FROM messages
GROUP BY user_id;

CREATE UNIQUE INDEX IF NOT EXISTS mv_top_users_unique
    ON mv_top_users (user_id);

-- MV 2: Per-room message count (backs Analytics 3: getTopRooms)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_rooms AS
SELECT room_id, COUNT(*) AS message_count
FROM messages
GROUP BY room_id;

CREATE UNIQUE INDEX IF NOT EXISTS mv_top_rooms_unique
    ON mv_top_rooms (room_id);

-- MV 3: Per-user room participation summary (backs Analytics 4: getUserParticipationPatterns)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_participation AS
SELECT user_id,
       COUNT(DISTINCT room_id) AS rooms_count,
       COUNT(*)                AS total_messages
FROM messages
GROUP BY user_id;

CREATE UNIQUE INDEX IF NOT EXISTS mv_user_participation_unique
    ON mv_user_participation (user_id);