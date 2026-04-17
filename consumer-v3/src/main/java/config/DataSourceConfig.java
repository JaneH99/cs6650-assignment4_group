package config;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import db.DatabaseWriter;
import db.MessageBuffer;
import db.MessageRepository;

/**
 * Multi-database configuration for the Consumer service.
 * DB1 (rooms 1-10): db1DataSource + jdbcTemplate + messageRepository1
 * DB2 (rooms 11-20): db2DataSource + jdbc2Template + messageRepository2
 */
@Configuration
public class DataSourceConfig {

  private static final Logger log = LoggerFactory.getLogger(DataSourceConfig.class);

  @Value("${db.writer.buffer-capacity}")
  private int bufferCapacity;

  @Value("${db.writer.threads:8}")
  private int writerThreads;

  @Value("${db.writer.batch-size}")
  private int batchSize;

  @Value("${db.writer.flush-interval-ms}")
  private long flushIntervalMs;

  @Value("${db.writer.max-retries:3}")
  private int maxRetries;

  @Value("${db.writer.metrics-interval-sec:5}")
  private int dbMetricsIntervalSec;

  // ======================== DB1 Configuration ========================

  @Value("${spring.datasource.url}")
  private String db1Url;

  @Value("${spring.datasource.username}")
  private String db1Username;

  @Value("${spring.datasource.password}")
  private String db1Password;

  @Value("${spring.datasource.hikari.maximum-pool-size:15}")
  private int db1MaxPoolSize;

  @Value("${spring.datasource.hikari.connection-init-sql:SET synchronous_commit=off}")
  private String db1InitSql;

  @Bean(name = "db1DataSource")
  @Primary
  public DataSource db1DataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(db1Url);
    config.setUsername(db1Username);
    config.setPassword(db1Password);
    config.setMaximumPoolSize(db1MaxPoolSize);
    config.setConnectionInitSql(db1InitSql);
    config.setPoolName("db1-hikari-pool");
    config.setAutoCommit(true);
    return new HikariDataSource(config);
  }

  @Bean(name = "jdbcTemplate")
  @Primary
  public JdbcTemplate jdbcTemplate(javax.sql.DataSource db1DataSource) {
    return new JdbcTemplate(db1DataSource);
  }

  @Bean(name = "messageRepository1")
  @Primary
  public MessageRepository messageRepository1(
      @Qualifier("jdbcTemplate") JdbcTemplate jdbcTemplate) {
    return new MessageRepository(jdbcTemplate);
  }

  // ======================== DB2 Configuration ========================

  @Value("${spring.datasource2.url}")
  private String db2Url;

  @Value("${spring.datasource2.username}")
  private String db2Username;

  @Value("${spring.datasource2.password}")
  private String db2Password;

  @Value("${spring.datasource2.hikari.maximum-pool-size:15}")
  private int db2MaxPoolSize;

  @Value("${spring.datasource2.hikari.connection-init-sql:SET synchronous_commit=off}")
  private String db2InitSql;

  @Bean(name = "db2DataSource")
  public DataSource db2DataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(db2Url);
    config.setUsername(db2Username);
    config.setPassword(db2Password);
    config.setMaximumPoolSize(db2MaxPoolSize);
    config.setConnectionInitSql(db2InitSql);
    config.setPoolName("db2-hikari-pool");
    config.setAutoCommit(true);
    return new HikariDataSource(config);
  }

  @Bean(name = "jdbc2Template")
  public JdbcTemplate jdbc2Template(javax.sql.DataSource db2DataSource) {
    return new JdbcTemplate(db2DataSource);
  }

  @Bean(name = "messageRepository2")
  public MessageRepository messageRepository2(
      @Qualifier("jdbc2Template") JdbcTemplate jdbc2Template) {
    return new MessageRepository(jdbc2Template);
  }

  // ======================== Dual Buffer Configuration ========================

  @Bean(name = "buffer1")
  public MessageBuffer buffer1() {
    log.info("Creating buffer1 (rooms 1-10) with capacity={}", bufferCapacity);
    return new MessageBuffer(bufferCapacity, "buffer1");
  }

  @Bean(name = "buffer2")
  public MessageBuffer buffer2() {
    log.info("Creating buffer2 (rooms 11-20) with capacity={}", bufferCapacity);
    return new MessageBuffer(bufferCapacity, "buffer2");
  }

  // ======================== Router Buffer ========================

  @Bean
  public MessageBuffer routerMessageBuffer(
      @Qualifier("buffer1") MessageBuffer buffer1,
      @Qualifier("buffer2") MessageBuffer buffer2) {
    return new RouterMessageBuffer(buffer1, buffer2);
  }

  /**
   * Routes messages to buffer1 (rooms 1-10) or buffer2 (rooms 11-20).
   */
  private static class RouterMessageBuffer extends MessageBuffer {
    private final MessageBuffer buffer1;
    private final MessageBuffer buffer2;

    public RouterMessageBuffer(MessageBuffer buffer1, MessageBuffer buffer2) {
      super(Integer.MAX_VALUE, "router");
      this.buffer1 = buffer1;
      this.buffer2 = buffer2;
    }

    @Override
    public boolean offer(model.BroadcastMessage message) {
      int roomNum = extractRoomNumber(message.getRoomId());
      if (roomNum >= 1 && roomNum <= 10) {
        return buffer1.offer(message);
      } else {
        return buffer2.offer(message);
      }
    }

    @Override
    public model.BroadcastMessage poll(long timeoutMs, java.util.concurrent.TimeUnit unit)
        throws InterruptedException {
      throw new UnsupportedOperationException("poll() not supported on router buffer");
    }

    @Override
    public int drainTo(java.util.List<model.BroadcastMessage> target, int maxElements) {
      throw new UnsupportedOperationException("drainTo() not supported on router buffer");
    }

    @Override
    public boolean isEmpty() {
      return buffer1.isEmpty() && buffer2.isEmpty();
    }

    @Override
    public int size() {
      return buffer1.size() + buffer2.size();
    }

    private int extractRoomNumber(String roomId) {
      if (roomId == null) return -1;
      try {
        return Integer.parseInt(roomId.replace("room", ""));
      } catch (NumberFormatException e) {
        return -1;
      }
    }
  }

  // ======================== Dual DatabaseWriter Configuration ========================

  @Bean(name = "databaseWriter1")
  public DatabaseWriter databaseWriter1(
      @Qualifier("buffer1") MessageBuffer buffer1,
      @Qualifier("messageRepository1") MessageRepository repository1) {
    return new DatabaseWriter(
        buffer1,
        repository1,
        writerThreads,
        batchSize,
        flushIntervalMs,
        maxRetries,
        dbMetricsIntervalSec);
  }

  @Bean(name = "databaseWriter2")
  public DatabaseWriter databaseWriter2(
      @Qualifier("buffer2") MessageBuffer buffer2,
      @Qualifier("messageRepository2") MessageRepository repository2) {
    return new DatabaseWriter(
        buffer2,
        repository2,
        writerThreads,
        batchSize,
        flushIntervalMs,
        maxRetries,
        dbMetricsIntervalSec);
  }
}
