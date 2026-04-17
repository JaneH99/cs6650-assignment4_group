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
 * Service for publishing messages to RabbitMQ.
 * All messages are published to a single RabbitMQ instance via ChannelPool.
 * A circuit breaker prevents publishing when RabbitMQ is unavailable.
 */
@Service
public class MessagePublisher {

  private static final Logger log = LoggerFactory.getLogger(MessagePublisher.class);

  private final ChannelPool channelPool;
  private final Gson gson = new Gson();

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
        clientMsg.getMessageId() != null ? clientMsg.getMessageId() : UUID.randomUUID().toString());
    broadcast.setRoomId(roomId);
    broadcast.setUserId(clientMsg.getUserId());
    broadcast.setUsername(clientMsg.getUsername());
    broadcast.setMessage(clientMsg.getMessage());
    broadcast.setMessageType(clientMsg.getMessageType());
    broadcast.setTimestamp(Instant.now().toString());
    broadcast.setServerId(serverId);
    String clientIp = (session.getRemoteAddress() != null)
        ? ((java.net.InetSocketAddress) session.getRemoteAddress()).getAddress().getHostAddress()
        : "unknown";
    broadcast.setClientIp(clientIp);

    String json = gson.toJson(broadcast);
    String routingKey = "room." + roomId;

    Channel channel = channelPool.borrowChannel();
    try {
      channel.basicPublish(
          RabbitMQConfig.EXCHANGE_NAME,
          routingKey,
          MessageProperties.BASIC,
          json.getBytes(StandardCharsets.UTF_8));
      log.debug("Published: messageId={} room={}", broadcast.getMessageId(), roomId);
    } finally {
      channelPool.returnChannel(channel);
    }
  }

  // Called when circuit is OPEN or when publish throws
  public void publishFallback(String roomId, WebSocketSession session, String rawJson,
      Exception e) {
    log.error("RabbitMQ circuit open — message dropped: room={} error={}", roomId, e.getMessage());
  }
}
