package db;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
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
import model.BroadcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;

/**
 * Write-behind DB writer. Runs a fixed configurable thread pool where each thread continuously:
 *   1. Waits up to flushIntervalMs for the first message
 *   2. Drains up to batchSize messages from the shared buffer
 *   3. Batch-inserts to Postgres with exponential backoff retries
 *   4. Routes permanently failed batches to dead letter log
 *
 * Phase = MAX_VALUE - 1: starts before RoomConsumer, stops after RoomConsumer,
 */
public class DatabaseWriter implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(DatabaseWriter.class);

  private final MessageBuffer buffer;
  private final MessageRepository repository;
  private final int writerThreads;
  private final int batchSize;
  private final long flushIntervalMs;
  private final int maxRetries;

  private ExecutorService writerPool;
  private volatile boolean running = false;

  private final int dbMetricsIntervalSec;
  private final AtomicLong recentWritten = new AtomicLong(0);
  private final AtomicLong totalWritten = new AtomicLong(0);
  private final ConcurrentLinkedQueue<Long> batchLatencies = new ConcurrentLinkedQueue<>();
  private final ScheduledExecutorService metricsScheduler =
      Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "db-metrics"));

  public DatabaseWriter(
      MessageBuffer buffer,
      MessageRepository repository,
      @Value("${db.writer.threads:8}") int writerThreads,
      @Value("${db.writer.batch-size}") int batchSize,
      @Value("${db.writer.flush-interval-ms}") long flushIntervalMs,
      @Value("${db.writer.max-retries:3}") int maxRetries,
      @Value("${db.writer.metrics-interval-sec:5}") int dbMetricsIntervalSec) {
    this.buffer = buffer;
    this.repository = repository;
    this.writerThreads = writerThreads;
    this.batchSize = batchSize;
    this.flushIntervalMs = flushIntervalMs;
    this.maxRetries = maxRetries;
    this.dbMetricsIntervalSec = dbMetricsIntervalSec;
  }

  @Override
  public void start() {
//    Create 8 writer threads and submit writerLoop for each thread
    ThreadFactory threadFactory = new ThreadFactory() {
      private final AtomicInteger count = new AtomicInteger(0);
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "db-writer-" + count.getAndIncrement());
      }
    };
    writerPool = Executors.newFixedThreadPool(writerThreads, threadFactory);
    for (int i = 0; i < writerThreads; i++) {
      writerPool.submit(this::writerLoop);
    }
    running = true;
    log.info("DatabaseWriter started: {} threads, batchSize={}, flushInterval={}ms",
        writerThreads, batchSize, flushIntervalMs);
//    Log write throughput
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
      log.info("[DB METRICS] dbThroughput={}/sec totalWritten={} queueDepth={} p50={}ms p95={}ms p99={}ms",
          written / dbMetricsIntervalSec, totalWritten.get(), buffer.size(), p50, p95, p99);
    }, dbMetricsIntervalSec, dbMetricsIntervalSec, TimeUnit.SECONDS);
  }

  /**
   * Each writer thread runs this loop independently.
   * Blocks up to flushIntervalMs waiting for the first message,
   * then drains up to batchSize before flushing.
   * This handles both triggers: batch-size-reached and flush-interval.
   */
  private void writerLoop() {
    List<BroadcastMessage> batch = new ArrayList<>(batchSize);
    while (running || !buffer.isEmpty()) {
      try {
//        Try to poll a message from the queue, if there are message(s), remove up to
//        batchsize number of messages from queue and append them to batch (list)
        BroadcastMessage first = buffer.poll(flushIntervalMs, TimeUnit.MILLISECONDS);
        if (first != null) {
          batch.add(first);
          buffer.drainTo(batch, batchSize - 1);
        }
//        Write batch to database
        if (!batch.isEmpty()) {
          writeWithRetry(batch);
          batch.clear();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }

//    Flush remaining messages in batch
    if (!batch.isEmpty()) {
      writeWithRetry(batch);
    }
    batch.clear();

//    Drain everything left in the queue
    buffer.drainTo(batch, Integer.MAX_VALUE);
    if (!batch.isEmpty()) {
      writeWithRetry(batch);
    }
  }

  /**
   * Retries with exponential backoff.
   * If the circuit breaker is open (CallNotPermittedException), skips retries immediately
   * and sends to dead letter
   * After maxRetries exhausted, also sends to dead letter.
   */
  private void writeWithRetry(List<BroadcastMessage> batch) {
    long backoffMs = 100;
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        long writeStart = System.currentTimeMillis();
        repository.batchInsert(batch);
        // Record write latency per batch. Each message in the batch experiences the full batch write
        // time as its write latency — dividing by batch size would be incorrect since all messages
        // are blocked until the entire batch insert completes.
        batchLatencies.add(System.currentTimeMillis() - writeStart);
        recentWritten.addAndGet(batch.size());
        totalWritten.addAndGet(batch.size());
        return;
      } catch (CallNotPermittedException e) {
        // Circuit is open — DB is unhealthy, stop retrying immediately
        log.warn("Circuit breaker open — skipping retries for {} messages", batch.size());
        sendToDeadLetter(batch);
        return;
      } catch (Exception e) {
        log.warn("DB write attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());
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

  private void sendToDeadLetter(List<BroadcastMessage> batch) {
    log.error("[DEAD LETTER] {} messages permanently failed — message_ids:", batch.size());
    batch.forEach(m -> log.error("[DEAD LETTER] message_id={} room={} user={}",
        m.getMessageId(), m.getRoomId(), m.getUserId()));
  }

  @Override
  public void stop() {
    running = false;
    metricsScheduler.shutdownNow();
    log.info("Stopping DatabaseWriter — draining remaining buffer ({} messages)...", buffer.size());
    if (writerPool != null) {
      writerPool.shutdown();
      try {
        writerPool.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    log.info("DatabaseWriter stopped");
  }

  @Override public boolean isRunning() { return running; }

  // Starts before RoomConsumer (MAX_VALUE), stops after RoomConsumer so drains buffer last
  @Override public int getPhase() { return Integer.MAX_VALUE - 1; }
}
