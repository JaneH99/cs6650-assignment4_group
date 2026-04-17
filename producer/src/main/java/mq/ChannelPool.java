package mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages a fixed pool of AMQP channels over a single shared Connection.
 * Channels are borrowed before publish and returned immediately after.
 *
 * Pool size should be set to numRooms * partitionsPerRoom so every
 * partition has a dedicated channel available without contention.
 * Default: 20 rooms * 3 partitions = 60 channels.
 *
 * closeAll() is called by Spring on shutdown via destroyMethod.
 */
public class ChannelPool {

  private static final Logger log = LoggerFactory.getLogger(ChannelPool.class);

  private static final int  STARTUP_MAX_RETRIES    = 15;
  private static final long STARTUP_RETRY_DELAY_MS = 3_000;

  private volatile Connection connection;
  private final BlockingQueue<Channel> pool;
  private final int poolSize;
  private final ConnectionFactory factory;
  private static final long BORROW_TIMEOUT_SECONDS = 1;

  public ChannelPool(String host, String username, String password, int poolSize)
          throws IOException, TimeoutException {
    this.poolSize = poolSize;
    this.pool = new ArrayBlockingQueue<>(poolSize);
    this.factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setUsername(username);
    factory.setPassword(password);
    factory.setAutomaticRecoveryEnabled(false);
    factory.setConnectionTimeout(3000);

    createConnectionWithRetry();
    initializePool();
    log.info("ChannelPool initialised: {} channels", poolSize);
  }

  private void createConnectionWithRetry() throws IOException, TimeoutException {
    int attempt = 0;
    while (true) {
      try {
        attempt++;
        log.info("Connecting to RabbitMQ (attempt {}/{})...", attempt, STARTUP_MAX_RETRIES);
        createConnection();
        log.info("Connected to RabbitMQ successfully.");
        return;
      } catch (IOException | TimeoutException e) {
        if (attempt >= STARTUP_MAX_RETRIES) {
          log.error("Failed to connect to RabbitMQ after {} attempts, giving up.",
                  STARTUP_MAX_RETRIES);
          throw e;
        }
        log.warn("RabbitMQ not ready yet ({}), retrying in {}ms...",
                e.getMessage(), STARTUP_RETRY_DELAY_MS);
        try {
          Thread.sleep(STARTUP_RETRY_DELAY_MS);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while waiting for RabbitMQ", ie);
        }
      }
    }
  }

  private synchronized void createConnection() throws IOException, TimeoutException {
    this.connection = factory.newConnection();
  }

  private void initializePool() throws IOException {
    for (int i = 0; i < poolSize; i++) {
      pool.add(connection.createChannel());
    }
  }

  public Channel borrowChannel() throws InterruptedException, IOException, TimeoutException {
    ensureConnectionAlive();
    Channel channel = pool.poll(BORROW_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    if (channel == null) {
      throw new RuntimeException("Timed out waiting for an available RabbitMQ channel");
    }
    if (!channel.isOpen()) {
      channel = connection.createChannel();
    }
    return channel;
  }

  public void returnChannel(Channel channel) {
    if (channel != null && channel.isOpen()) {
      boolean returned = pool.offer(channel);
      if (!returned) {
        try {
          channel.close();
        } catch (Exception ignored) {}
      }
    }
  }

  private void ensureConnectionAlive() throws IOException, TimeoutException {
    if (connection == null || !connection.isOpen()) {
      synchronized (this) {
        if (connection == null || !connection.isOpen()) {
          closeAll();
          createConnection();
          initializePool();
        }
      }
    }
  }

  public void closeAll() {
    log.info("Closing ChannelPool");
    for (Channel channel : pool) {
      try { channel.close(); } catch (Exception ignored) {}
    }
    pool.clear();
    try { connection.close(); } catch (Exception ignored) {}
  }
}