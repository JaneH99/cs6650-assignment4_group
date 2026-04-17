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
 * Batch processing:
 *   Messages accumulate in a per-channel ChannelBatch. The batch flushes when: it reaches BATCH_SIZE, or
 *   the periodic flusher fires (every FLUSH_INTERVAL_MS).
 *   This prevents partial batches from sitting unACKed forever.
 *
 * Room assignment (round-robin):
 *   effectiveChannels = min(consumerThreads, numRooms)
 *   Channel i handles all rooms where (roomId - 1) % effectiveChannels == i
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
  private final MessageProcessingService processingService;

  // Flush when batch reaches this size (must be < prefetchCount to avoid deadlock)
  private static final int BATCH_SIZE = 40;
  // Flush partial batches at this interval to prevent unACKed message buildup
  private static final long FLUSH_INTERVAL_MS = 60;

  private Connection connection;
  private final List<Channel> channels = new ArrayList<>();
  private final List<ChannelBatch> batches = new ArrayList<>();
  private ScheduledExecutorService batchFlusher;
  private volatile boolean running = false;

  public RoomConsumer(String host, String username, String password,
      int consumerThreads, int prefetchCount, int numRooms,
      int roomsStart, int roomsEnd,
      MessageProcessingService processingService) {
    this.host = host;
    this.username = username;
    this.password = password;
    this.consumerThreads = consumerThreads;
    this.prefetchCount = prefetchCount;
    this.numRooms = numRooms;
    this.roomsStart = roomsStart;
    this.roomsEnd = roomsEnd;
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
//      One connection per Consumer
      connection = factory.newConnection("room-consumer");

//      Load test: one consumerThread per room is optimal.
      int effectiveChannels = Math.min(consumerThreads, roomsEnd - roomsStart + 1);
      for (int i = 0; i < effectiveChannels; i++) {
        startConsumerChannel(i, effectiveChannels);
      }

      // Periodically flush partial batches so they don't sit unACKed
      batchFlusher = Executors.newSingleThreadScheduledExecutor(
          r -> new Thread(r, "batch-flusher"));
      batchFlusher.scheduleAtFixedRate(this::flushAllBatches,
          FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);

      running = true;
      log.info("RoomConsumer started: {} channels, rooms={}-{}, batchSize={}, flushInterval={}ms",
          effectiveChannels, roomsStart, roomsEnd, BATCH_SIZE, FLUSH_INTERVAL_MS);
    } catch (IOException | TimeoutException e) {
      throw new RuntimeException("Failed to start RoomConsumer", e);
    }
  }

  /**
   * Creates one AMQP channel and subscribes it to its assigned room(s).
   * Each channel gets a ChannelBatch that accumulates messages until
   * BATCH_SIZE is reached or the periodic flusher fires.
   */
  private void startConsumerChannel(int index, int effectiveChannels) throws IOException {
    Channel channel = connection.createChannel();
    channel.basicQos(prefetchCount);

    ChannelBatch batch = new ChannelBatch(channel, processingService);
    batches.add(batch);

    for (int roomId = roomsStart; roomId <= roomsEnd; roomId++) {
//      One or multiple rooms are assigned to one channel, so one channel only deals with messages from specific room's queue(s), message order is preserved for each room
      if ((roomId - roomsStart) % effectiveChannels == index) {
        String queueName = "room." + roomId;
//        RabbitMQ pushes messages to this channel asynchronously.
//        We register a consumer callback to handle incoming messages from the queue.
        channel.basicConsume(queueName, false,
            new DefaultConsumer(channel) {
              @Override
              public void handleDelivery(String consumerTag, Envelope envelope,
                  BasicProperties properties, byte[] body) {
//                Add to batches instead of processing it right away
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
    // Shutdown flusher and do one final flush of all partial batches
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
   * add() is called by ConsumerWorkService; flush() by both ConsumerWorkService
   * and the batch-flusher timer.
   */
  private static class ChannelBatch {

    private final Channel channel;
    private final MessageProcessingService processingService;
    private final List<Long> deliveryTags = new ArrayList<>(BATCH_SIZE);
    private final List<byte[]> bodies = new ArrayList<>(BATCH_SIZE);
    // Time when the first message in this batch arrived from RabbitMQ
    private long batchReceiveTime = 0;

    ChannelBatch(Channel channel, MessageProcessingService processingService) {
      this.channel = channel;
      this.processingService = processingService;
    }

    synchronized void add(long deliveryTag, byte[] body) {
      if (deliveryTags.isEmpty()) {
        batchReceiveTime = System.currentTimeMillis();  // first message arrival time
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
