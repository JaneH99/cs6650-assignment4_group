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
 * Consumes messages from per-partition RabbitMQ queues and delegates to
 * MessageProcessingService for Redis publish → ACK/NACK.
 *
 * Queue partitioning optimization:
 *   Previously each room had one queue consumed by one channel (serial).
 *   Now each room has partitionsPerRoom queues. Each partition gets its own
 *   consumer channel, so multiple partitions in the same room are consumed
 *   in parallel, increasing throughput for hot rooms.
 *
 * Ordering trade-off:
 *   Per-user ordering is preserved: hash(userId) % partitionsPerRoom routes
 *   the same user's messages to the same partition consistently, and each
 *   partition is consumed serially by one channel.
 *   Cross-user global ordering is not guaranteed — this is the same semantic
 *   as Kafka's per-partition ordering guarantee, and is acceptable for a
 *   high-throughput chat system where delivery latency matters more than
 *   strict interleaving order across users.
 *
 * Partition assignment (round-robin across channels):
 *   All partitions are flattened into a list ordered by room then partition.
 *   Channel i handles all partitions where partitionIndex % effectiveChannels == i.
 *
 * Batch processing:
 *   Messages accumulate in a per-channel ChannelBatch. The batch flushes when
 *   it reaches BATCH_SIZE or the periodic flusher fires every FLUSH_INTERVAL_MS.
 *
 * SmartLifecycle (phase = MAX_VALUE):
 *   Starts LAST — after RedisPublisher is fully ready.
 *   Stops FIRST — flushes partial batches before closing channels.
 */
public class RoomConsumer implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(RoomConsumer.class);

  private final String host;
  private final String username;
  private final String password;
  private final int consumerThreads;
  private final int prefetchCount;
  private final int numRooms;
  private final int roomsStart;
  private final int roomsEnd;
  private final int partitionsPerRoom;
  private final MessageProcessingService processingService;

  private static final int  BATCH_SIZE        = 40;
  private static final long FLUSH_INTERVAL_MS = 60;

  private Connection connection;
  private final List<Channel> channels = new ArrayList<>();
  private final List<ChannelBatch> batches = new ArrayList<>();
  private ScheduledExecutorService batchFlusher;
  private volatile boolean running = false;

  public RoomConsumer(String host, String username, String password,
                      int consumerThreads, int prefetchCount, int numRooms,
                      int roomsStart, int roomsEnd,
                      int partitionsPerRoom,
                      MessageProcessingService processingService) {
    this.host = host;
    this.username = username;
    this.password = password;
    this.consumerThreads = consumerThreads;
    this.prefetchCount = prefetchCount;
    this.numRooms = numRooms;
    this.roomsStart = roomsStart;
    this.roomsEnd = roomsEnd;
    this.partitionsPerRoom = partitionsPerRoom;
    this.processingService = processingService;
  }

  @Override
  public void start() {
    try {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost(host);
      factory.setUsername(username);
      factory.setPassword(password);
      factory.setSharedExecutor(Executors.newFixedThreadPool(consumerThreads));
      connection = factory.newConnection("room-consumer");

      int totalPartitions = (roomsEnd - roomsStart + 1) * partitionsPerRoom;
      int effectiveChannels = Math.min(consumerThreads, totalPartitions);

      for (int i = 0; i < effectiveChannels; i++) {
        startConsumerChannel(i, effectiveChannels);
      }

      batchFlusher = Executors.newSingleThreadScheduledExecutor(
              r -> new Thread(r, "batch-flusher"));
      batchFlusher.scheduleAtFixedRate(this::flushAllBatches,
              FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);

      running = true;
      log.info("RoomConsumer started: {} channels, rooms={}-{}, partitionsPerRoom={}, "
                      + "batchSize={}, flushInterval={}ms",
              effectiveChannels, roomsStart, roomsEnd, partitionsPerRoom,
              BATCH_SIZE, FLUSH_INTERVAL_MS);
    } catch (IOException | TimeoutException e) {
      throw new RuntimeException("Failed to start RoomConsumer", e);
    }
  }

  /**
   * Creates one AMQP channel and subscribes it to its assigned partitions.
   *
   * All (room, partition) pairs are flattened into a sequential index:
   *   index 0 = room.roomsStart.p.0
   *   index 1 = room.roomsStart.p.1
   *   index 2 = room.roomsStart.p.2
   *   index 3 = room.(roomsStart+1).p.0
   *   ...
   *
   * Channel i takes every partition where partitionIndex % effectiveChannels == i,
   * distributing load evenly across channels.
   */
  private void startConsumerChannel(int index, int effectiveChannels) throws IOException {
    Channel channel = connection.createChannel();
    channel.basicQos(prefetchCount);

    ChannelBatch batch = new ChannelBatch(channel, processingService);
    batches.add(batch);

    int partitionIndex = 0;
    for (int roomId = roomsStart; roomId <= roomsEnd; roomId++) {
      for (int p = 0; p < partitionsPerRoom; p++) {
        if (partitionIndex % effectiveChannels == index) {
          String queueName = "room." + roomId + ".p." + p;
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
        partitionIndex++;
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
    try { connection.close(); } catch (Exception ignored) {}
    log.info("RoomConsumer stopped");
  }

  @Override public boolean isRunning() { return running; }

  /**
   * Holds per-channel batch state.
   * add() is called by RabbitMQ ConsumerWorkService threads.
   * flush() is called by both ConsumerWorkService and the batch-flusher timer.
   * Both methods are synchronized to prevent concurrent modification.
   */
  private static class ChannelBatch {

    private final Channel channel;
    private final MessageProcessingService processingService;
    private final List<Long>   deliveryTags = new ArrayList<>(BATCH_SIZE);
    private final List<byte[]> bodies       = new ArrayList<>(BATCH_SIZE);
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