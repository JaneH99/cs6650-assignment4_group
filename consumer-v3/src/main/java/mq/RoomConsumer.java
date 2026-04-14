package mq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.AMQP.BasicProperties;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import service.MessageProcessingService;

/**
 * Consumes messages from per-room RabbitMQ queues and delegates to
 * MessageProcessingService for Redis publish → ACK/NACK.
 *
 * This consumer connects to TWO RabbitMQ instances:
 *   Connection-1 → rooms 1-10 (host)
 *   Connection-2 → rooms 11-20 (host2)
 * Each connection gets its own independent channel pool.
 *
 * Batch processing:
 *   Messages accumulate in a per-channel ChannelBatch. The batch flushes when:
 *   it reaches BATCH_SIZE, or the periodic flusher fires (every FLUSH_INTERVAL_MS).
 *
 * Room assignment within each connection (round-robin):
 *   effectiveChannels = min(consumerThreads, numRoomsPerConnection)
 *   Channel i handles all rooms where (roomId - start) % effectiveChannels == i
 *
 * SmartLifecycle (phase = MAX_VALUE):
 *   Starts LAST — after RedisPublisher is fully ready.
 *   Stops FIRST — flushes partial batches before closing channels.
 */
public class RoomConsumer implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(RoomConsumer.class);

  private final String host;
  private final String host2;
  private final String username;
  private final String password;
  private final int consumerThreads;
  private final int prefetchCount;
  private final int roomsStart;
  private final int roomsEnd;
  private final int roomsStart2;
  private final int roomsEnd2;
  private final MessageProcessingService processingService;

  // Flush when batch reaches this size (must be < prefetchCount to avoid deadlock)
  private static final int BATCH_SIZE = 40;
  // Flush partial batches at this interval to prevent unACKed message buildup
  private static final long FLUSH_INTERVAL_MS = 60;

  private final List<Connection> connections = new ArrayList<>();
  private final List<Channel> channels = new ArrayList<>();
  private final List<ChannelBatch> batches = new ArrayList<>();
  private ScheduledExecutorService batchFlusher;
  private volatile boolean running = false;

  public RoomConsumer(String host, String host2, String username, String password,
      int consumerThreads, int prefetchCount,
      int roomsStart, int roomsEnd,
      int roomsStart2, int roomsEnd2,
      MessageProcessingService processingService) {
    this.host = host;
    this.host2 = host2;
    this.username = username;
    this.password = password;
    this.consumerThreads = consumerThreads;
    this.prefetchCount = prefetchCount;
    this.roomsStart = roomsStart;
    this.roomsEnd = roomsEnd;
    this.roomsStart2 = roomsStart2;
    this.roomsEnd2 = roomsEnd2;
    this.processingService = processingService;
  }

  @Override
  public void start() {
    try {
      // Connection-1 → RabbitMQ-1 (rooms 1-10)
      startConnection(host, roomsStart, roomsEnd, "conn-1");
      // Connection-2 → RabbitMQ-2 (rooms 11-20)
      startConnection(host2, roomsStart2, roomsEnd2, "conn-2");

      // Periodically flush partial batches so they don't sit unACKed
      batchFlusher = Executors.newSingleThreadScheduledExecutor(
          r -> new Thread(r, "batch-flusher"));
      batchFlusher.scheduleAtFixedRate(this::flushAllBatches,
          FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);

      running = true;
      log.info("RoomConsumer started: 2 connections, conn-1 rooms={}-{} conn-2 rooms={}-{}",
          roomsStart, roomsEnd, roomsStart2, roomsEnd2);
    } catch (IOException | TimeoutException e) {
      throw new RuntimeException("Failed to start RoomConsumer", e);
    }
  }

  /**
   * Creates one RabbitMQ Connection and starts consumer channels for its assigned rooms.
   */
  private void startConnection(String targetHost, int start, int end, String connName)
      throws IOException, TimeoutException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(targetHost);
    factory.setUsername(username);
    factory.setPassword(password);
    factory.setSharedExecutor(Executors.newFixedThreadPool(consumerThreads));

    Connection conn = factory.newConnection(connName);
    connections.add(conn);

    int effectiveChannels = Math.min(consumerThreads, end - start + 1);
    for (int i = 0; i < effectiveChannels; i++) {
      startConsumerChannel(conn, i, effectiveChannels, start, end);
    }
    log.info("Connection {} started: host={} rooms={}-{} channels={}",
        connName, targetHost, start, end, effectiveChannels);
  }

  /**
   * Creates one AMQP channel and subscribes it to its assigned room(s).
   * Each channel gets a ChannelBatch that accumulates messages until
   * BATCH_SIZE is reached or the periodic flusher fires.
   */
  private void startConsumerChannel(Connection conn, int index, int effectiveChannels,
      int roomStart, int roomEnd) throws IOException {
    Channel channel = conn.createChannel();
    channel.basicQos(prefetchCount);

    ChannelBatch batch = new ChannelBatch(channel, processingService);
    batches.add(batch);

    for (int roomId = roomStart; roomId <= roomEnd; roomId++) {
      // Round-robin assignment: channel i handles rooms where (roomId - roomStart) % effectiveChannels == i
      if ((roomId - roomStart) % effectiveChannels == index) {
        String queueName = "room." + roomId;
        channel.basicConsume(queueName, false,
            new DefaultConsumer(channel) {
              @Override
              public void handleDelivery(String consumerTag, Envelope envelope,
                  BasicProperties properties, byte[] body) {
                batch.add(envelope.getDeliveryTag(), body);
              }
            });
        log.debug("Channel {} subscribed to queue: {}", index, queueName);
      }
    }
    channels.add(channel);
  }

  private void flushAllBatches() {
    for (ChannelBatch batch : batches) {
      batch.flush();
    }
  }

  @Override
  public void stop() {
    running = false;
    log.info("Stopping RoomConsumer...");
    if (batchFlusher != null) {
      batchFlusher.shutdown();
    }
    flushAllBatches();
    for (Channel ch : channels) {
      try { ch.close(); } catch (Exception ignored) {}
    }
    for (Connection conn : connections) {
      try { conn.close(); } catch (Exception ignored) {}
    }
    log.info("RoomConsumer stopped");
  }

  @Override public boolean isRunning() { return running; }

  /**
   * Holds per-channel batch state.
   */
  private static class ChannelBatch {

    private final Channel channel;
    private final MessageProcessingService processingService;
    private final List<Long> deliveryTags = new ArrayList<>(BATCH_SIZE);
    private final List<byte[]> bodies = new ArrayList<>(BATCH_SIZE);
    private long batchReceiveTime = 0;

    ChannelBatch(Channel channel, MessageProcessingService processingService) {
      this.channel = channel;
      this.processingService = processingService;
    }

    synchronized void add(long deliveryTag, byte[] body) {
      if (deliveryTags.isEmpty()) {
        batchReceiveTime = System.currentTimeMillis();
      }
      deliveryTags.add(deliveryTag);
      bodies.add(body);
      if (deliveryTags.size() >= BATCH_SIZE) {
        flush();
      }
    }

    synchronized void flush() {
      if (deliveryTags.isEmpty()) return;
      processingService.processBatch(channel, deliveryTags, bodies, batchReceiveTime);
      deliveryTags.clear();
      bodies.clear();
      batchReceiveTime = 0;
    }
  }
}
