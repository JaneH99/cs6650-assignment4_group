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
 * A single Connection is shared; channels are borrowed before publish
 * and returned immediately.
 *
 * closeAll() is called by Spring on shutdown via destroyMethod.
 */
public class ChannelPool {

  private static final Logger log = LoggerFactory.getLogger(ChannelPool.class);

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
//    Disable auto-recovery — manual reconnection in ensureConnectionAlive() handles this
    factory.setAutomaticRecoveryEnabled(false);
//    Fail fast so threads don't pile up behind the synchronized lock when broker is unreachable
    factory.setConnectionTimeout(3000);
    createConnection();
    initializePool();
    log.info("ChannelPool initialised: {} channels", poolSize);
  }

  /**
   *  Use synchronized to avoid multiple threads trying to create connections simultaneously when encountering connection failure
   */
  private synchronized void createConnection() throws IOException, TimeoutException {
    this.connection = factory.newConnection();
  }

  /**
   *  Initialize connection pool channels
   */
  private void initializePool() throws IOException {
    for (int i = 0; i < poolSize; i++) {
      pool.add(connection.createChannel());
    }
  }

  /**
   *   Borrow a channel. wait up to a timeout instead of blocking indefinitely
   */
  public Channel borrowChannel() throws InterruptedException, IOException, TimeoutException {
    ensureConnectionAlive();
//    Wait up to BORROW_TIMEOUT_SECONDS instead of blocking indefinitely by using take()
    Channel channel = pool.poll(BORROW_TIMEOUT_SECONDS, TimeUnit.SECONDS);
//    If cannot borrow a channel, throw exception to inform no available RabbitMQ channel
    if (channel == null) {
      throw new RuntimeException("Timed out waiting for an available RabbitMQ channel");
    }
//    If channel is not open, create new channel
    if (!channel.isOpen()) {
      channel = connection.createChannel();
    }
    return channel;
  }

  /**
   *  Return a channel to the pool.
   */
  public void returnChannel(Channel channel) {
    if (channel != null && channel.isOpen()) {
      boolean returned = pool.offer(channel);
//      pool is full, close the channel. It happens when a channel is borrowed before reconnection. When it tries to return, initializePool() already filled the pool
      if (!returned) {
        try {
          channel.close();
        } catch (Exception ignored) {}
      }
    }
  }

  /**
   * Ensure connection is open, if no, restart everything
   */
  private void ensureConnectionAlive() throws IOException, TimeoutException {
//    If connection is not working, close all and recreate connection and re-initialize channel pool
    if (connection == null || !connection.isOpen()) {
      synchronized (this) {
//        Double check to avoid duplicate reconnection attempts
        if (connection == null || !connection.isOpen()) {
          closeAll();
          createConnection();
          initializePool();
        }
      }
    }
  }

  /**
   * Close all pooled channels and the shared connection.
   * Called by Spring on application shutdown.
   */
  public void closeAll() {
    log.info("Closing ChannelPool");
//    Close all channels
    for (Channel channel : pool) {
      try { channel.close(); } catch (Exception ignored) {}
    }
//    Clear pool
    pool.clear();
//    Close connection
    try { connection.close(); } catch (Exception ignored) {}
  }
}
