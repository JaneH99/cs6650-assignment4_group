package testing;

import db.MessageRepository;
import java.util.ArrayList;
import java.util.List;
import model.BroadcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Tests DB write throughput at batch sizes: 100, 500, 1000, 5000.
 * Messages are pre-generated before timing starts — generation cost excluded.
 * Only runs when db.batch-size-test.enabled=true.
 *
 * To run test: add db.batch-size-test.enabled=true to application.properties, then start consumer.
 * Then check consumer.log for result. grep "BATCH SIZE TEST" consumer.log
 */
@Component
@ConditionalOnProperty(name = "db.batch-size-test.enabled", havingValue = "true")
public class BatchSizeTest implements CommandLineRunner {

  private static final Logger log = LoggerFactory.getLogger(BatchSizeTest.class);
  private static final int[] BATCH_SIZES = {100, 500, 1000, 5000};

  private final MessageRepository repository;
  private final JdbcTemplate jdbc;

  @Value("${db.batch-size-test.total-messages:500000}")
  private int totalMessages;

  public BatchSizeTest(MessageRepository repository, JdbcTemplate jdbc) {
    this.repository = repository;
    this.jdbc = jdbc;
  }

  @Override
  public void run(String... args) {
    log.info("=================================================");
    log.info("[BATCH SIZE TEST] total messages per run: {}", totalMessages);
    log.info("=================================================");

    // Pre-generate all messages once — excluded from timing
    log.info("[BATCH SIZE TEST] Pre-generating {} messages...", totalMessages);
    List<BroadcastMessage> allMessages = generateMessages(totalMessages);
    log.info("[BATCH SIZE TEST] Generation complete. Starting tests...");

    log.info("[BATCH SIZE TEST] batchSize | duration(ms) | avgMsgLatency(ms) | throughput(msg/s)");

    for (int batchSize : BATCH_SIZES) {
      truncate();
      runTest(allMessages, batchSize);
    }

    log.info("=================================================");
    log.info("[BATCH SIZE TEST] Complete.");
    log.info("=================================================");
  }

  /**
   * Splits pre-generated messages into batches of batchSize and inserts them to DB.
   * Timer starts immediately — no message generation inside.
   */
  private void runTest(List<BroadcastMessage> messages, int batchSize) {
    int batches = 0;
    long totalBatchLatency = 0;
    long start = System.currentTimeMillis();

    for (int i = 0; i < messages.size(); i += batchSize) {
      List<BroadcastMessage> batch = messages.subList(i, Math.min(i + batchSize, messages.size()));
      long batchStart = System.currentTimeMillis();
      repository.batchInsert(batch);
      totalBatchLatency += System.currentTimeMillis() - batchStart;
      batches++;
    }

    long duration = System.currentTimeMillis() - start;
    long throughput = (totalMessages * 1000L) / duration;
    long avgMsgLatencyMs = batches > 0 ? totalBatchLatency / batches : 0;

    log.info("[BATCH SIZE TEST] batchSize={} duration={}ms avgMsgLatency={}ms throughput={}msg/sec",
        batchSize, duration, avgMsgLatencyMs, throughput);
  }

  private List<BroadcastMessage> generateMessages(int count) {
    List<BroadcastMessage> messages = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      BroadcastMessage msg = new BroadcastMessage();
      msg.setMessageId("batch-size-test-" + i);
      msg.setRoomId("room-" + (i % 20 + 1));
      msg.setUserId("user-" + (i % 100 + 1));
      msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
      msg.setMessage("Batch size test message " + i);
      messages.add(msg);
    }
    return messages;
  }

  private void truncate() {
    jdbc.execute("TRUNCATE TABLE messages");
  }
}
