package service;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisStringCommands;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;

import db.MessageBuffer;
import model.BroadcastMessage;
import redis.RedisPublisher;

/**
 * Orchestrates the processing pipeline for each RabbitMQ delivery:
 *   1. Deserialize JSON
 *   2. Publish to Redis Pub/Sub (circuit-broken)
 *   3. ACK RabbitMQ on success
 *   4. NACK on failure (circuit open or Redis error)
 * With manual ACK and NACK, it ensures At-least-once delivery
 * Called from RoomConsumer on a dedicated AMQP consumer thread per channel.
 * Thread-safe: AtomicLong counters, stateless dependencies.
 */
@Service
public class MessageProcessingService {

  private static final Logger log = LoggerFactory.getLogger(MessageProcessingService.class);
  private static final long NACK_BACKOFF_MS = 500;
  private static final int  METRICS_INTERVAL_SEC = 5;

  private static final Duration DEDUP_TTL = Duration.ofMinutes(5);

  private final RedisPublisher redisPublisher;
  private final MessageBuffer messageBuffer;
  private final StringRedisTemplate redisTemplate;
  private final Gson gson = new Gson();

//  Mark it as false for Endurance test
  @Value("${redis.publish.enabled:true}")
  private boolean redisPublishEnabled;

  private final AtomicLong processed = new AtomicLong(0);
  private final AtomicLong failures = new AtomicLong(0);

  // Rolling counters reset every METRICS_INTERVAL_SEC for rate calculation
  private final AtomicLong recentProcessed = new AtomicLong(0);
  private final AtomicLong recentFailures = new AtomicLong(0);
  // Running sum and count for average consumer lag per interval
  private final AtomicLong lagSumMs = new AtomicLong(0);
  private final AtomicLong lagCount = new AtomicLong(0);

  public MessageProcessingService(RedisPublisher redisPublisher,
      @Qualifier("routerMessageBuffer") MessageBuffer messageBuffer,
      StringRedisTemplate redisTemplate) {
    this.redisPublisher = redisPublisher;
    this.messageBuffer = messageBuffer;
    this.redisTemplate = redisTemplate;
    startMetricsLogger();
  }

  /**
   * Logs throughput (msg/sec) and average consumer lag every METRICS_INTERVAL_SEC seconds.
   * Consumer lag = time from producer publish timestamp to consumer processing time.
   */
  private void startMetricsLogger() {
    Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "metrics-logger"))
        .scheduleAtFixedRate(() -> {
          long msgs = recentProcessed.getAndSet(0);
          long errs = recentFailures.getAndSet(0);
          long count = lagCount.getAndSet(0);
          long sumMs = lagSumMs.getAndSet(0);
          long avgLag = count > 0 ? sumMs / count : 0;
          log.info("[METRICS] throughput={}/sec failures={}/sec avgConsumerLag={}ms",
              msgs / METRICS_INTERVAL_SEC, errs / METRICS_INTERVAL_SEC, avgLag);
        }, METRICS_INTERVAL_SEC, METRICS_INTERVAL_SEC, TimeUnit.SECONDS);
  }

  /**
   * Process messaged in batches will improve through put but increase single message latency, compared to processing messages one by one.
   * Trade-off: can only detect success and failure at batch level. And some messaged may be delivered more than once, which is ok based on at-least-once delicery requirement
   * Processes a batch of messages:
   *   1. Deserialize all bodies
   *   2. Publish all to Redis in one pipelined round-trip
   *   3. Batch ACK the last deliveryTag (multiple=true covers all prior unacked tags)
   *   4. Batch NACK on failure (requeue=true for at-least-once delivery)
   */
  public void processBatch(Channel channel, List<Long> deliveryTags, List<byte[]> bodies, long batchReceiveTime) {
    List<BroadcastMessage> messages = new ArrayList<>(bodies.size());
    List<String> jsons = new ArrayList<>(bodies.size());
    long lastTag = deliveryTags.get(deliveryTags.size() - 1);

    for (byte[] body : bodies) {
      String json = new String(body, StandardCharsets.UTF_8);
      BroadcastMessage msg = gson.fromJson(json, BroadcastMessage.class);
      if (msg != null && msg.getMessageId() != null) {
        messages.add(msg);
        jsons.add(json);
      } else {
        log.warn("Malformed message in batch — discarding");
      }
    }

    if (messages.isEmpty()) {
      batchAck(channel, lastTag);
      return;
    }

    try {
      if (redisPublishEnabled) {
        // Deduplicate: filter out messages already published (from prior redelivery)
        filterDuplicates(messages, jsons);
        if (messages.isEmpty()) {
          batchAck(channel, lastTag);
          return;
        }

        // Group by roomId — one Redis publish per room instead of one per message
        Map<String, List<String>> byRoom = new LinkedHashMap<>();
        for (int i = 0; i < messages.size(); i++) {
          byRoom.computeIfAbsent(messages.get(i).getRoomId(), k -> new ArrayList<>())
              .add(jsons.get(i));
        }
        for (Map.Entry<String, List<String>> entry : byRoom.entrySet()) {
          redisPublisher.publishRoomBatch(entry.getKey(), entry.getValue());
        }
      }
      batchAck(channel, lastTag);
      processed.addAndGet(messages.size());
      recentProcessed.addAndGet(messages.size());

      // Write-behind: push to DB buffer after ACK — DB writes are decoupled from this thread
      for (BroadcastMessage msg : messages) {
        // Back-pressure: block consumer thread until buffer has space (may cause RabbitMQ memory alarm under high load - in Endurance Test)
        // It is used for 1,000,000 load test to make sure all messages write to DB
        // try {
        //   if (!messageBuffer.offer(msg, 2000, TimeUnit.MILLISECONDS)) {
        //     log.warn("DB buffer full after 2s — dropping message_id={}", msg.getMessageId());
        //   }
        // } catch (InterruptedException ie) {
        //   Thread.currentThread().interrupt();
        //   break;
        // }
        if (!messageBuffer.offer(msg)) {
          log.warn("DB buffer full — dropping message_id={}", msg.getMessageId());
        }
      }

      // Consumer lag: time from first message received (handleDelivery) → Redis publish done
      // Measures: batch buffer wait + deserialization + Redis publish.
      long lagMs = System.currentTimeMillis() - batchReceiveTime;
      lagSumMs.addAndGet(lagMs * messages.size());
      lagCount.addAndGet(messages.size());
      log.debug("Batch processed: {} messages", messages.size());
    } catch (Exception e) {
      log.error("Batch failed for {} messages — NACKing: {}", bodies.size(), e.getMessage());
      failures.addAndGet(bodies.size());
      recentFailures.addAndGet(bodies.size());
      batchNackWithBackoff(channel, lastTag);
    }
  }

  // ---- Metrics ----

  public long getProcessed() { return processed.get(); }
  public long getFailures()  { return failures.get(); }

  public void resetCounters() {
    processed.set(0);
    failures.set(0);
    recentProcessed.set(0);
    recentFailures.set(0);
    lagSumMs.set(0);
    lagCount.set(0);
    log.info("[METRICS] Counters reset");
  }

  // ---- Private helpers ----

  /**
   * Removes duplicates from both lists using a Redis pipeline.
   * Pipeline sends all SETNX commands in one round trip, returns true/false per message.
   * Filter both lists based on results — only keep messages where SETNX returned true.
   * TTL is 5 min — long enough to cover any RabbitMQ redelivery window
   */
  private void filterDuplicates(List<BroadcastMessage> messages, List<String> jsons) {
//    Queue commands locally and send all commands to Redis in one batch
    List<Object> results = redisTemplate.executePipelined(
        (org.springframework.data.redis.core.RedisCallback<Object>) conn -> {
          StringRedisConnection strConn = (StringRedisConnection) conn;
          for (BroadcastMessage msg : messages) {
            strConn.set("dedup:" + msg.getMessageId(), "1",
                Expiration.seconds(DEDUP_TTL.getSeconds()),
                RedisStringCommands.SetOption.SET_IF_ABSENT);
          }
          return null; // return value ignored in pipeline mode
        });

    List<BroadcastMessage> dedupedMessages = new ArrayList<>(messages.size());
    List<String> dedupedJsons = new ArrayList<>(jsons.size());
    for (int i = 0; i < messages.size(); i++) {
      if (Boolean.TRUE.equals(results.get(i))) {
        dedupedMessages.add(messages.get(i));
        dedupedJsons.add(jsons.get(i));
      } else {
        log.debug("Duplicate msg: message_id={}", messages.get(i).getMessageId());
      }
    }
    messages.clear();
    messages.addAll(dedupedMessages);
    jsons.clear();
    jsons.addAll(dedupedJsons);
  }

  private void batchAck(Channel channel, long lastDeliveryTag) {
    try {
      channel.basicAck(lastDeliveryTag, true);  // multiple=true: ACKs all up to lastDeliveryTag
    } catch (IOException e) {
      log.error("Failed to batch ACK up to tag={}", lastDeliveryTag, e);
    }
  }

  private void batchNackWithBackoff(Channel channel, long lastDeliveryTag) {
    try {
      channel.basicNack(lastDeliveryTag, true, true);  // multiple=true, requeue=true
    } catch (IOException e) {
      log.error("Failed to batch NACK up to tag={}", lastDeliveryTag, e);
    }
    try {
      Thread.sleep(NACK_BACKOFF_MS);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
