package db;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/**
 * Executes all core and analytics queries for the Metrics API.
 */
@Repository
public class MetricsRepository {

  private static final Logger log = LoggerFactory.getLogger(MetricsRepository.class);
  private final int topN;
  private final JdbcTemplate jdbc;
  // Add Materialized View Refresher
  private final MaterializedViewRefresher mvRefresher;

  public MetricsRepository(JdbcTemplate jdbc, @Value("${metrics.top-n}") int topN,
      MaterializedViewRefresher mvRefresher) {
    this.jdbc = jdbc;
    this.topN = topN;
    this.mvRefresher = mvRefresher;
  }

  // ---- Core Queries ----

  /**
   * Query 1: Get messages for a room in time range, ordered by timestamp.
   * Performance target: < 100ms for 1000 messages.
   * Uses index: idx_messages_room_timestamp
   */
  public List<Map<String, Object>> getRoomMessages(String roomId, String startTime, String endTime) {
    return jdbc.queryForList(
        "SELECT message_id, room_id, user_id, timestamp, content " +
        "FROM messages WHERE room_id = ? AND timestamp >= ? AND timestamp <= ? " +
        "ORDER BY timestamp",
        roomId, startTime, endTime);
  }

  /**
   * Query 2: Get user's message history, optionally filtered by date range.
   * Performance target: < 200ms.
   * Uses index: idx_messages_user_room_timestamp
   */
  public List<Map<String, Object>> getUserHistory(String userId, String startTime, String endTime) {
    if (startTime != null && endTime != null) {
      return jdbc.queryForList(
          "SELECT message_id, room_id, user_id, timestamp, content " +
          "FROM messages WHERE user_id = ? AND timestamp >= ? AND timestamp <= ? " +
          "ORDER BY timestamp",
          userId, startTime, endTime);
    }
    return jdbc.queryForList(
        "SELECT message_id, room_id, user_id, timestamp, content " +
        "FROM messages WHERE user_id = ? ORDER BY timestamp",
        userId);
  }

  /**
   * Query 3: Count unique active users in a time window.
   * Performance target: < 500ms.
   * Uses index: idx_messages_timestamp
   */
  public int countActiveUsers(String startTime, String endTime) {
    Integer count = jdbc.queryForObject(
        "SELECT COUNT(DISTINCT user_id) " +
            "FROM messages WHERE timestamp >= ? AND timestamp <= ?",
        Integer.class, startTime, endTime);
    return count != null ? count : 0;
  }

  /**
   * Query 4: Get rooms a user has participated in, with last activity timestamp.
   * Performance target: < 50ms.
   * Uses covering index: idx_messages_user_room_timestamp
   */
  public List<Map<String, Object>> getRoomsForUser(String userId) {
    return jdbc.queryForList(
        "SELECT room_id, MAX(timestamp) as last_activity " +
        "FROM messages WHERE user_id = ? GROUP BY room_id",
        userId);
  }

  // ---- Analytics Queries ----

  /**
   * Analytics 1: Messages per second/minute statistics.
   * Computes total messages, time span, and derived rates.
   */
  @Cacheable("messageStats")
  public Map<String, Object> getMessageStats() {
    log.info("[CACHE MISS] querying message stats from DB");
    Map<String, Object> stats = jdbc.queryForMap(
        "SELECT COUNT(*) as total_messages, " +
        "MIN(timestamp) as earliest_timestamp, " +
        "MAX(timestamp) as latest_timestamp " +
        "FROM messages");

    // Derive messages/sec and messages/min from time span
    Object earliest = stats.get("earliest_timestamp");
    Object latest = stats.get("latest_timestamp");
    Object total = stats.get("total_messages");

    if (earliest != null && latest != null && total != null) {
      long spanMs = toEpochMilli(latest.toString()) - toEpochMilli(earliest.toString());
      if (spanMs > 0) {
        double msgPerSec = Long.parseLong(total.toString()) * 1000.0 / spanMs;
        double msgPerMin = msgPerSec * 60;
        stats.put("messages_per_sec", Math.round(msgPerSec));
        stats.put("messages_per_min", Math.round(msgPerMin));
        stats.put("duration_ms", spanMs);
      }
    }
    return stats;
  }

  /**
   * Analytics 2: Top N most active users by message count.
   * Reads from mv_top_users (pre-aggregated) instead of scanning messages.
   */
  @Cacheable("topUsers")
  public List<Map<String, Object>> getTopUsers() {
    log.info("[CACHE MISS] querying top users from materialized view");
    return jdbc.queryForList(
        "SELECT user_id, message_count FROM mv_top_users " +
            "ORDER BY message_count DESC LIMIT ?",
        topN);
  }

  /**
   * Analytics 3: Top N most active rooms by message count.
   * Reads from mv_top_rooms (pre-aggregated) instead of scanning messages.
   */
  @Cacheable("topRooms")
  public List<Map<String, Object>> getTopRooms() {
    log.info("[CACHE MISS] querying top rooms from materialized view");
    return jdbc.queryForList(
        "SELECT room_id, message_count FROM mv_top_rooms " +
            "ORDER BY message_count DESC LIMIT ?",
        topN);
  }

  /**
   * Analytics 4: User participation patterns, interpretered by how many rooms each user participated in
   * and how many messages each user sent in total
   * Reads from mv_user_participation (pre-aggregated) instead of scanning messages.
   */
  @Cacheable("participationPatterns")
  public List<Map<String, Object>> getUserParticipationPatterns() {
    log.info("[CACHE MISS] querying user participation patterns from materialized view");
    return jdbc.queryForList(
        "SELECT user_id, rooms_count, total_messages FROM mv_user_participation " +
            "ORDER BY rooms_count DESC LIMIT ?",
        topN);
  }

  private long toEpochMilli(String timestamp) {
    try {
      return Long.parseLong(timestamp);
    } catch (NumberFormatException e) {
      return Instant.parse(timestamp).toEpochMilli();
    }
  }

  public void truncate() {
    jdbc.execute("TRUNCATE TABLE messages");
    // Refresh MVs immediately after truncation so they reflect the empty table.
    // Uses direct (non-concurrent) refresh since no concurrent readers matter during reset.
    mvRefresher.refreshAllDirect();
    log.info("messages table truncated and materialized views refreshed");
  }

  public long getTotalMessages() {
    Long count = jdbc.queryForObject("SELECT COUNT(*) FROM messages", Long.class);
    return count != null ? count : 0;
  }
}
