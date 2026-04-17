package testing;

import db.MessageBuffer;
import model.BroadcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Batch test for the DB writer. Test for 1) DB write 2) idempotent write
 * Bypasses RabbitMQ and Redis — pushes messages directly into MessageBuffer.
 * Only runs when db.test.enabled=true.
 *
 * To run test: add db.test.enabled=true to application.properties, then start consumer.
 * Check logs for results: grep "DB TEST" consumer.log, then query DB to verify.
 */
@Component
@ConditionalOnProperty(name = "db.test.enabled", havingValue = "true")
public class DbWriterTest implements CommandLineRunner {

  private static final Logger log = LoggerFactory.getLogger(DbWriterTest.class);

  private final MessageBuffer buffer;
  private final JdbcTemplate jdbc;

  @Value("${db.test.message-count:1000}")
  private int messageCount;

  @Value("${db.writer.flush-interval-ms:500}")
  private long flushIntervalMs;

  public DbWriterTest(@Qualifier("routerMessageBuffer") MessageBuffer buffer, JdbcTemplate jdbc) {
    this.buffer = buffer;
    this.jdbc = jdbc;
  }

  @Override
  public void run(String... args) throws Exception {
    jdbc.execute("TRUNCATE TABLE messages");
    log.info("[DB TEST] Truncated messages table before test starts");
    log.info("[DB TEST] Pushing {} messages into buffer...", messageCount);

    for (int i = 0; i < messageCount; i++) {
      BroadcastMessage msg = new BroadcastMessage();
      msg.setMessageId("test-msg-" + i);
      msg.setRoomId("room-" + (i % 20 + 1));
      msg.setUserId("user-" + (i % 100 + 1));
      msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
      msg.setMessage("Test message " + i);
      buffer.offer(msg);
    }

    log.info("[DB TEST] All {} messages pushed. Waiting for writer to flush...", messageCount);

    // Wait long enough for all batches to flush
    Thread.sleep(flushIntervalMs * 3);

    // Verify count in DB
    Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM messages", Integer.class);
    log.info("[DB TEST] Messages in DB: {} / {} expected", count, messageCount);

    // Test idempotency: push the same messages again
    log.info("[DB TEST] Re-pushing same {} messages to test idempotency...", messageCount);
    for (int i = 0; i < messageCount; i++) {
      BroadcastMessage msg = new BroadcastMessage();
      msg.setMessageId("test-msg-" + i);  // same IDs
      msg.setRoomId("room-" + (i % 20 + 1));
      msg.setUserId("user-" + (i % 100 + 1));
      msg.setTimestamp(String.valueOf(System.currentTimeMillis()));
      msg.setMessage("Duplicate message " + i);
      buffer.offer(msg);
    }

    Thread.sleep(flushIntervalMs * 3);

    Integer countAfterDup = jdbc.queryForObject("SELECT COUNT(*) FROM messages", Integer.class);
    log.info("[DB TEST] Messages in DB after duplicate push: {} (should still be {})",
        countAfterDup, messageCount);

    if (count != null && count.equals(countAfterDup)) {
      log.info("[DB TEST] PASSED — idempotent writes working correctly");
    } else {
      log.error("[DB TEST] FAILED — duplicate messages were inserted");
    }
  }
}
