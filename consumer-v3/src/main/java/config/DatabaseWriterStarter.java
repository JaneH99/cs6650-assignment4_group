package config;

import db.DatabaseWriter;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;

/**
 * Ensures DatabaseWriter beans are started on application startup.
 * Spring only auto-starts SmartLifecycle beans that are referenced by other beans.
 */
@Configuration
public class DatabaseWriterStarter {

  private static final Logger log = LoggerFactory.getLogger(DatabaseWriterStarter.class);

  private final DatabaseWriter databaseWriter1;
  private final DatabaseWriter databaseWriter2;

  public DatabaseWriterStarter(
      @Qualifier("databaseWriter1") DatabaseWriter databaseWriter1,
      @Qualifier("databaseWriter2") DatabaseWriter databaseWriter2) {
    this.databaseWriter1 = databaseWriter1;
    this.databaseWriter2 = databaseWriter2;
    log.info("DatabaseWriterStarter constructed: writer1={}, writer2={}",
        databaseWriter1, databaseWriter2);
  }

  @PostConstruct
  public void init() {
    log.info("DatabaseWriterStarter.init() called - starting writers...");
    try {
      log.info("Starting databaseWriter1...");
      databaseWriter1.start();
      log.info("databaseWriter1 started successfully, isRunning={}", databaseWriter1.isRunning());

      log.info("Starting databaseWriter2...");
      databaseWriter2.start();
      log.info("databaseWriter2 started successfully, isRunning={}", databaseWriter2.isRunning());

      log.info("All DatabaseWriter beans started successfully");
    } catch (Exception e) {
      log.error("Failed to start DatabaseWriter", e);
      throw e;
    }
  }
}
