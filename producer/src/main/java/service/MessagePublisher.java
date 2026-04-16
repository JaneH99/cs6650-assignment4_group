package service;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import config.RabbitMQConfig;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import model.BroadcastMessage;
import model.ClientMessage;
import mq.ChannelPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.WebSocketSession;

/**
 * Service for publishing messages to RabbitMQ using a pool of reusable channels.
 * A circuit breaker (configured in application.properties) is applied to prevent
 * failures when RabbitMQ is unavailable.
 *
 * Queue partitioning optimization:
 *   The routing key is computed as "room.{roomId}.p.{partition}" where partition
 *   is derived from hash(userId) % partitionsPerRoom. This ensures:
 *   1. Messages from the same user always go to the same partition (stable routing).
 *   2. Multiple partitions in one room can be consumed in parallel by different
 *      consumer channels, increasing throughput for hot rooms.
 *
 * Ordering trade-off:
 *   Per-user ordering is preserved because the same userId always maps to the
 *   same partition, and each partition is consumed serially by one channel.
 *   Cross-user global ordering is not guaranteed — identical to Kafka's
 *   per-partition ordering semantic — and is acceptable for a high-throughput
 *   chat system where latency matters more than strict interleaving order.
 */
@Service
public class MessagePublisher {

  private static final Logger log = LoggerFactory.getLogger(MessagePublisher.class);

  private final ChannelPool channelPool;
  private final Gson gson = new Gson();

  // Must match the value declared in producer RabbitMQConfig and consumer RabbitMQConfig
  @Value("${rabbitmq.partitions.per.room:3}")
  private int partitionsPerRoom;

  @Value("${server.id:producer-1}")
  private String serverId;

  public MessagePublisher(ChannelPool channelPool) {
    this.channelPool = channelPool;
  }

  @CircuitBreaker(name = "rabbitmq-publish", fallbackMethod = "publishFallback")
  public void publish(String roomId, WebSocketSession session, String rawJson) throws Exception {
    ClientMessage clientMsg = gson.fromJson(rawJson, ClientMessage.class);

    BroadcastMessage broadcast = new BroadcastMessage();
    broadcast.setMessageId(
            clientMsg.getMessageId() != null
                    ? clientMsg.getMessageId()
                    : UUID.randomUUID().toString());
    broadcast.setRoomId(roomId);
    broadcast.setUserId(clientMsg.getUserId());
    broadcast.setUsername(clientMsg.getUsername());
    broadcast.setMessage(clientMsg.getMessage());
    broadcast.setMessageType(clientMsg.getMessageType());
    broadcast.setTimestamp(Instant.now().toString());
    broadcast.setServerId(serverId);
    String clientIp = (session.getRemoteAddress() != null)
            ? ((java.net.InetSocketAddress) session.getRemoteAddress())
            .getAddress().getHostAddress()
            : "unknown";
    broadcast.setClientIp(clientIp);

    String json = gson.toJson(broadcast);

    // Partition selection: Math.abs guards against negative hashCode values.
    // Same userId always maps to the same partition, preserving per-user
    // message order within a partition while allowing cross-partition
    // parallelism across users in the same room.
    int partition = Math.abs(clientMsg.getUserId().hashCode()) % partitionsPerRoom;
    String routingKey = "room." + roomId + ".p." + partition;

    Channel channel = channelPool.borrowChannel();
    try {
      channel.basicPublish(
              RabbitMQConfig.EXCHANGE_NAME,
              routingKey,
              MessageProperties.BASIC,
              json.getBytes(StandardCharsets.UTF_8));
      log.debug("Published: messageId={} room={} partition={}",
              broadcast.getMessageId(), roomId, partition);
    } finally {
      channelPool.returnChannel(channel);
    }
  }

  public void publishFallback(String roomId, WebSocketSession session, String rawJson,
                              Exception e) {
    log.error("RabbitMQ circuit open — message dropped: room={} error={}",
            roomId, e.getMessage());
  }
}