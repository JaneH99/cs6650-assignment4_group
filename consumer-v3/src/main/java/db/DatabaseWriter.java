package db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import model.BroadcastMessage;

/**
 * Write-behind DB writer with sharded database support.
 * Runs two fixed thread pools (one per DB shard):
 *   Pool-1: writes to DB1 (rooms 1-10) from db1Buffer
 *   Pool-2: writes to DB2 (rooms 11-20) from db2Buffer
 * Each thread continuously:
 *   1. Waits up to flushIntervalMs for the first message
 *   2. Drains up to batchSize messages from its buffer
 *   3. Batch-inserts to the corresponding Postgres shard
 *   4. Routes permanently failed batches to dead letter log
 *
 * Phase = MAX_VALUE - 1: starts before RoomConsumer, stops after RoomConsumer,
 */
@Component
public class DatabaseWriter implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(DatabaseWriter.class);

  private final MessageBuffer db1Buffer;
  private final MessageBuffer db2Buffer;
  private final JdbcTemplate db1Jdbc;
  private final JdbcTemplate db2Jdbc;
  private final int writerThreads;
  private final int batchSize;
  private final long flushIntervalMs;
  private final int maxRetries;

  private ExecutorService db1WriterPool;
  private ExecutorService db2WriterPool;
  private volatile boolean running = false;

  private final int dbMetricsIntervalSec;
  private final AtomicLong recentWritten = new AtomicLong(0);
  private final AtomicLong totalWritten = new AtomicLong(0);
  private final ConcurrentLinkedQueue<Long> batchLatencies = new ConcurrentLinkedQueue<>();
  private final ScheduledExecutorService metricsScheduler =
      Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "db-metrics"));

  @Autowired
  public DatabaseWriter(
      @Qualifier("db1Buffer") MessageBuffer db1Buffer,
      @Qualifier("db2Buffer") MessageBuffer db2Buffer,
      @Qualifier("dataSource1") DataSource dataSource1,
      @Qualifier("dataSource2") DataSource dataSource2,
      @Value("${db.writer.threads:8}") int writerThreads,
      @Value("${db.writer.batch-size}") int batchSize,
      @Value("${db.writer.flush-interval-ms}") long flushIntervalMs,
      @Value("${db.writer.max-retries:3}") int maxRetries,
      @Value("${db.writer.metrics-interval-sec:5}") int dbMetricsIntervalSec) {
    this.db1Buffer = db1Buffer;
    this.db2Buffer = db2Buffer;
    this.db1Jdbc = new JdbcTemplate(dataSource1);
    this.db2Jdbc = new JdbcTemplate(dataSource2);
    this.writerThreads = writerThreads;
    this.batchSize = batchSize;
    this.flushIntervalMs = flushIntervalMs;
    this.maxRetries = maxRetries;
    this.dbMetricsIntervalSec = dbMetricsIntervalSec;
  }

  @Override
  public void start() {
    running = true;
    ThreadFactory threadFactory = new ThreadFactory() {
      private final AtomicInteger count = new AtomicInteger(0);
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "db-writer-" + count.getAndIncrement());
      }
    };

    // Split threads evenly between DB1 and DB2
    int threadsPerDb = writerThreads / 2;

    db1WriterPool = Executors.newFixedThreadPool(threadsPerDb, threadFactory);
    db2WriterPool = Executors.newFixedThreadPool(threadsPerDb, threadFactory);

    for (int i = 0; i < threadsPerDb; i++) {
      db1WriterPool.submit(() -> writerLoop(db1Buffer, db1Jdbc, "DB1"));
      db2WriterPool.submit(() -> writerLoop(db2Buffer, db2Jdbc, "DB2"));
    }

    
    log.info("DatabaseWriter started: {} threads per DB, batchSize={}, flushInterval={}ms",
        threadsPerDb, batchSize, flushIntervalMs);

    metricsScheduler.scheduleAtFixedRate(() -> {
      long written = recentWritten.getAndSet(0);
      List<Long> snapshot = new ArrayList<>(batchLatencies);
      batchLatencies.clear();
      long p50 = 0, p95 = 0, p99 = 0;
      if (!snapshot.isEmpty()) {
        Collections.sort(snapshot);
        p50 = snapshot.get((int) (snapshot.size() * 0.50));
        p95 = snapshot.get((int) (snapshot.size() * 0.95));
        p99 = snapshot.get((int) (snapshot.size() * 0.99));
      }
      log.info("[DB METRICS] throughput={}/sec totalWritten={} db1Queue={} db2Queue={} p50={}ms p95={}ms p99={}ms",
          written / dbMetricsIntervalSec, totalWritten.get(), db1Buffer.size(), db2Buffer.size(), p50, p95, p99);
    }, dbMetricsIntervalSec, dbMetricsIntervalSec, TimeUnit.SECONDS);
  }

  /**
   * Each writer thread runs this loop independently for its assigned buffer.
   */
  private void writerLoop(MessageBuffer buffer, JdbcTemplate jdbcTemplate, String dbName) {
    List<BroadcastMessage> batch = new ArrayList<>(batchSize);

    while (running || !buffer.isEmpty()) {
      try {
        // 设定一个截止时间：当前时间 + 最大等待时间
        long deadline = System.currentTimeMillis() + flushIntervalMs;

        // 核心循环：只要批次没满，或者时间还没到，就一直尝试捞消息
        while (batch.size() < batchSize) {
          long remainingTime = deadline - System.currentTimeMillis();
          if (remainingTime <= 0) {
            break; // ⏱️ 时间到了！不管手里有几条，立刻跳出循环去写库
          }

          BroadcastMessage msg = buffer.poll(remainingTime, TimeUnit.MILLISECONDS);
          if (msg == null) {
            break; // 队列真的一滴都没有了，跳出循环写库
          }

          batch.add(msg);

          // 极致优化：如果等待期间队列里又攒了几条，瞬间全部榨干，少等一会儿
          if (batch.size() < batchSize && !buffer.isEmpty()) {
            buffer.drainTo(batch, batchSize - batch.size());
          }
        }

        // 执行真正的批量写入
        if (!batch.isEmpty()) {
          writeWithRetry(batch, jdbcTemplate, dbName);
          batch.clear();
        }

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

    // 程序关闭时，把缓冲池里剩下的清理干净
    buffer.drainTo(batch, Integer.MAX_VALUE);
    if (!batch.isEmpty()) {
      writeWithRetry(batch, jdbcTemplate, dbName);
    }
  }

  /**
   * Retries with exponential backoff.
   */
  private void writeWithRetry(List<BroadcastMessage> batch, JdbcTemplate jdbcTemplate, String dbName) {
    if (batch.isEmpty()) return;

    long backoffMs = 100;
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        long writeStart = System.currentTimeMillis();
        batchInsert(jdbcTemplate, batch);
        batchLatencies.add(System.currentTimeMillis() - writeStart);
        recentWritten.addAndGet(batch.size());
        totalWritten.addAndGet(batch.size());
        return;
      } catch (CallNotPermittedException e) {
        log.warn("Circuit breaker open for {} — skipping retries for {} messages", dbName, batch.size());
        sendToDeadLetter(batch);
        return;
      } catch (Exception e) {
        log.warn("DB write attempt {}/{} failed for {}: {}", attempt, maxRetries, dbName, e.getMessage());
        if (attempt < maxRetries) {
          try { Thread.sleep(backoffMs); } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return;
          }
          backoffMs *= 2;
        }
      }
    }
    sendToDeadLetter(batch);
  }

  private static final String SQL =
      "INSERT INTO messages (message_id, room_id, user_id, timestamp, content) " +
      "VALUES (?, ?, ?, ?, ?) ON CONFLICT (message_id) DO NOTHING";

  private void batchInsert(JdbcTemplate jdbcTemplate, List<BroadcastMessage> msgs) {
    jdbcTemplate.batchUpdate(SQL, msgs, msgs.size(), (ps, msg) -> {
      ps.setString(1, msg.getMessageId());
      ps.setString(2, msg.getRoomId());
      ps.setString(3, msg.getUserId());
      ps.setString(4, msg.getTimestamp());
      ps.setString(5, msg.getMessage());
    });
  }

  private void sendToDeadLetter(List<BroadcastMessage> batch) {
    log.error("[DEAD LETTER] {} messages permanently failed:", batch.size());
    batch.forEach(m -> log.error("[DEAD LETTER] message_id={} room={} user={}",
        m.getMessageId(), m.getRoomId(), m.getUserId()));
  }

  @Override
  public void stop() {
    running = false;
    metricsScheduler.shutdownNow();
    log.info("Stopping DatabaseWriter...");

    if (db1WriterPool != null) db1WriterPool.shutdown();
    if (db2WriterPool != null) db2WriterPool.shutdown();

    try {
      if (db1WriterPool != null) db1WriterPool.awaitTermination(30, TimeUnit.SECONDS);
      if (db2WriterPool != null) db2WriterPool.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    log.info("DatabaseWriter stopped");
  }

  @Override public boolean isRunning() { return running; }

  @Override public int getPhase() { return Integer.MAX_VALUE - 1; }
}
