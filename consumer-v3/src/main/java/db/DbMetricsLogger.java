package db;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Periodically queries PostgreSQL system views to collect database metrics:
 * - Queries per second
 * - Active connections
 * - Lock wait time
 * - Buffer pool hit ratio
 * - Disk I/O statistics
 *
 * Only runs when db.metrics.enabled=true.
 *
 * Compatible with PostgreSQL 15 (pg_stat_checkpointer does not exist in PG15,
 * checkpoint stats are read from pg_stat_bgwriter instead).
 */
@Component
public class DbMetricsLogger {

  private static final Logger log = LoggerFactory.getLogger(DbMetricsLogger.class);

  private final JdbcTemplate jdbc;
  private final boolean enabled;
  private final int intervalSec;

  private final ScheduledExecutorService scheduler =
          Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "db-metrics-logger"));

  // Previous counters for delta calculations
  private long prevXactCommit    = -1;
  private long prevXactRollback  = -1;
  private long prevBlksRead      = -1;
  private long prevBlkReadTime   = -1;
  private long prevBlkWriteTime  = -1;
  private long prevBufCheckpoint = -1;
  private long prevBufClean      = -1;
  private long prevBufBackend    = -1;

  public DbMetricsLogger(
          JdbcTemplate jdbc,
          @Value("${db.metrics.enabled:false}") boolean enabled,
          @Value("${db.metrics.interval-sec:30}") int intervalSec) {
    this.jdbc = jdbc;
    this.enabled = enabled;
    this.intervalSec = intervalSec;
    if (enabled) {
      scheduler.scheduleAtFixedRate(this::collectSafely, intervalSec, intervalSec, TimeUnit.SECONDS);
      log.info("DbMetricsLogger started: interval={}s", intervalSec);
    }
  }

  private void collectSafely() {
    try {
      collect();
    } catch (Exception e) {
      log.warn("[DB IO] collection failed: {}", e.getMessage());
    }
  }

  public void stop() {
    scheduler.shutdownNow();
  }

  private void collect() {
    try {
      // -------------------------------
      // Active connections
      // -------------------------------
      Integer activeConns = jdbc.queryForObject(
              "SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active'",
              Integer.class);

      // -------------------------------
      // Lock wait (point-in-time)
      // -------------------------------
      Map<String, Object> lockStats = jdbc.queryForMap(
              "SELECT COALESCE(MAX(EXTRACT(EPOCH FROM (NOW() - state_change)) * 1000), 0) as max_lock_wait_ms, " +
                      "COALESCE(AVG(EXTRACT(EPOCH FROM (NOW() - state_change)) * 1000), 0) as avg_lock_wait_ms " +
                      "FROM pg_stat_activity WHERE wait_event_type = 'Lock'");
      long maxLockWaitMs = toLong(lockStats.get("max_lock_wait_ms"));
      long avgLockWaitMs = toLong(lockStats.get("avg_lock_wait_ms"));

      // -------------------------------
      // Core DB stats
      // -------------------------------
      Map<String, Object> dbStats = jdbc.queryForMap(
              "SELECT blks_hit, blks_read, xact_commit, xact_rollback, " +
                      "       blk_read_time, blk_write_time " +
                      "FROM pg_stat_database WHERE datname = current_database()");

      // Number of buffer hits (reads served from PostgreSQL shared buffer cache).
      long blksHit      = toLong(dbStats.get("blks_hit"));
      // Number of disk block reads (data not found in cache, had to be read from disk).
      long blksRead     = toLong(dbStats.get("blks_read"));
      // Number of committed transactions.
      long xactCommit   = toLong(dbStats.get("xact_commit"));
      // Number of rolled-back (aborted) transactions.
      long xactRollback = toLong(dbStats.get("xact_rollback"));
      // Total time (in milliseconds) spent reading data blocks from disk.
      long blkReadTime  = toLong(dbStats.get("blk_read_time"));
      // Total time (in milliseconds) spent writing data blocks to disk.
      long blkWriteTime = toLong(dbStats.get("blk_write_time"));

      // Compute deltas before updating prev values
      long deltaCommit    = prevXactCommit   >= 0 ? xactCommit   - prevXactCommit   : 0;
      long deltaRollback  = prevXactRollback >= 0 ? xactRollback - prevXactRollback : 0;
      long deltaBlksRead  = prevBlksRead     >= 0 ? blksRead     - prevBlksRead     : 0;
      long deltaWriteTime = prevBlkWriteTime >= 0 ? blkWriteTime - prevBlkWriteTime : 0;

      // QPS (transactions per second)
      long   qps         = (deltaCommit + deltaRollback) / intervalSec;
      // Disk read throughput (blocks per second)
      long   readsPerSec = deltaBlksRead / intervalSec;
      // Buffer cache hit ratio (%)
      double hitRatio    = (blksHit + blksRead) > 0 ? blksHit * 100.0 / (blksHit + blksRead) : 0;

      prevXactCommit   = xactCommit;
      prevXactRollback = xactRollback;
      prevBlksRead     = blksRead;
      prevBlkReadTime  = blkReadTime;
      prevBlkWriteTime = blkWriteTime;

      // -------------------------------
      // bgwriter / checkpoint stats
      // PG15: pg_stat_checkpointer 不存在，checkpoint 数据在 pg_stat_bgwriter 里
      // PG17: pg_stat_checkpointer 独立出来，pg_stat_bgwriter 里不再有这些字段
      // 这里用 pg_stat_bgwriter 兼容 PG15
      // -------------------------------
      Map<String, Object> bgWriter = jdbc.queryForMap(
              "SELECT buffers_clean, buffers_checkpoint, checkpoints_timed, checkpoints_req " +
                      "FROM pg_stat_bgwriter");

      // buffers_backend 在 PG15 也在 pg_stat_bgwriter 里
      long bufCheckpoint    = toLong(bgWriter.get("buffers_checkpoint"));
      long bufClean         = toLong(bgWriter.get("buffers_clean"));
      long checkpointsTimed = toLong(bgWriter.get("checkpoints_timed"));
      long checkpointsReq   = toLong(bgWriter.get("checkpoints_req"));

      // buffers_backend: PG15 在 pg_stat_bgwriter，PG17 移到了 pg_stat_io
      // 用 CASE WHEN 动态兼容两个版本
      long bufBackend = queryBuffersBackendCompat();

      // Compute deltas
      long buffersCheckpoint = prevBufCheckpoint >= 0 ? bufCheckpoint - prevBufCheckpoint : 0;
      long buffersClean      = prevBufClean      >= 0 ? bufClean      - prevBufClean      : 0;
      long buffersBackend    = prevBufBackend    >= 0 ? bufBackend    - prevBufBackend    : 0;

      prevBufCheckpoint = bufCheckpoint;
      prevBufClean      = bufClean;
      prevBufBackend    = bufBackend;

      // Write latency
      long   totalBufsWritten = buffersCheckpoint + buffersClean + buffersBackend;
      double writeLatencyMs   = totalBufsWritten > 0 ? deltaWriteTime * 1.0 / totalBufsWritten : 0;

      // Backend write pressure
      double backendWriteRatio = (buffersClean + buffersBackend) > 0
              ? buffersBackend * 100.0 / (buffersClean + buffersBackend)
              : 0;

      // -------------------------------
      // Disk size + JVM heap
      // -------------------------------
      String dbSize = jdbc.queryForObject(
              "SELECT pg_size_pretty(pg_database_size(current_database()))", String.class);

      Runtime rt = Runtime.getRuntime();
      long heapUsedMb = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);

      // -------------------------------
      // Log
      // -------------------------------
      log.info("[DB IO] qps={} hitRatio={}% activeConns={} lockWaitAvgMs={} lockWaitMaxMs={} dbSize={} heapUsed={}MB",
              qps, String.format("%.2f", hitRatio), activeConns, avgLockWaitMs, maxLockWaitMs, dbSize, heapUsedMb);

      log.info("[DB IO DISK] writeLatencyMs={} buffersClean={} buffersBackend={} backendWriteRatio={}% buffersCheckpoint={} checkpointsTimed={} checkpointsReq={}",
              String.format("%.2f", writeLatencyMs),
              buffersClean, buffersBackend,
              String.format("%.1f", backendWriteRatio),
              buffersCheckpoint, checkpointsTimed, checkpointsReq);

    } catch (Exception e) {
      log.warn("[DB IO] Failed to collect: {}", e.getMessage());
    }
  }

  /**
   * 兼容 PG15 和 PG17 读取 buffers_backend。
   * PG15: buffers_backend 在 pg_stat_bgwriter
   * PG17: buffers_backend 移到了 pg_stat_io，pg_stat_bgwriter 里该列已删除
   */
  private long queryBuffersBackendCompat() {
    try {
      // 先尝试 PG17 的 pg_stat_io（PG15 没有此视图，会抛异常）
      Long val = jdbc.queryForObject(
              "SELECT COALESCE(SUM(writes), 0) FROM pg_stat_io " +
                      "WHERE backend_type = 'client backend' AND object = 'relation'",
              Long.class);
      return val != null ? val : 0L;
    } catch (Exception e) {
      // 降级到 PG15 的 pg_stat_bgwriter
      try {
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT buffers_backend FROM pg_stat_bgwriter");
        return toLong(row.get("buffers_backend"));
      } catch (Exception ex) {
        log.warn("[DB IO] Could not read buffers_backend: {}", ex.getMessage());
        return 0L;
      }
    }
  }

  private long toLong(Object val) {
    if (val == null) return 0;
    return ((Number) val).longValue();
  }
}