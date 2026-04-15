package config;

import db.MessageBuffer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Configuration for sharded database buffers.
 * Two independent buffers route messages to different DB shards:
 *   db1Buffer: rooms 1-10 → DB1
 *   db2Buffer: rooms 11-20 → DB2
 */
@Configuration
public class BufferConfig {

  @Value("${db.writer.buffer-capacity:500000}")
  private int bufferCapacity;

  @Bean(name = "db1Buffer")
  @Primary
  public MessageBuffer db1Buffer() {
    return new MessageBuffer(bufferCapacity, "db1");
  }

  @Bean(name = "db2Buffer")
  public MessageBuffer db2Buffer() {
    return new MessageBuffer(bufferCapacity, "db2");
  }
}
