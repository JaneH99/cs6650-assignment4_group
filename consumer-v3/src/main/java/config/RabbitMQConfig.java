package config;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.util.HashMap;
import java.util.Map;
import mq.RoomConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import service.MessageProcessingService;

/**
 * Configures RabbitMQ topology and the RoomConsumer bean.
 *
 * Topology is declared Idempotently on startup.
 * Both Message Consumer and Message Producer declare the same exchange/queues with the same args, so whichever
 * starts first wins; the second is a no-op. If it is manually or automatically declared in RabbitMQ, both of them will be no-op
 */
@Configuration
public class RabbitMQConfig {

  public static final String EXCHANGE_NAME = "chat.exchange";

  @Value("${spring.rabbitmq.host}")
  private String host;

  @Value("${spring.rabbitmq2.host}")
  private String host2;

  @Value("${spring.rabbitmq.username:guest}")
  private String username;

  @Value("${spring.rabbitmq.password:guest}")
  private String password;

  @Value("${rabbitmq.rooms:20}")
  private int numRooms;

  @Value("${rabbitmq.rooms.start:1}")
  private int roomsStart;

  @Value("${rabbitmq.rooms.end:20}")
  private int roomsEnd;

  @Value("${rabbitmq.rooms2.start:11}")
  private int roomsStart2;

  @Value("${rabbitmq.rooms2.end:20}")
  private int roomsEnd2;

  @Value("${rabbitmq.queue.message-ttl:60000}")
  private int messageTtl;

  @Value("${rabbitmq.queue.max-length:10000}")
  private int maxLength;

  @Value("${rabbitmq.consumer.threads:4}")
  private int consumerThreads;

  @Value("${rabbitmq.consumer.prefetch:20}")
  private int prefetchCount;

  /**
   * Declares topology (exchange + queues + bindings) using a temporary connection.
   * Separate from the consumer connection so startup failures are isolated.
   * Make sure everything is declared before RoomConsumer starts
   */
  @Bean
  public RoomConsumer roomConsumer(MessageProcessingService processingService) throws Exception {
    declareTopologyForRange(host, roomsStart, roomsEnd);
    declareTopologyForRange(host2, roomsStart2, roomsEnd2);
    return new RoomConsumer(host, host2, username, password,
        consumerThreads, prefetchCount,
        roomsStart, roomsEnd, roomsStart2, roomsEnd2,
        processingService);
  }

  private void declareTopologyForRange(String targetHost, int start, int end) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(targetHost);
    factory.setUsername(username);
    factory.setPassword(password);

    try (Connection conn = factory.newConnection("topology-init");
         Channel channel = conn.createChannel()) {
      channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true);

      Map<String, Object> args = new HashMap<>();
      args.put("x-message-ttl", messageTtl);
      args.put("x-max-length", maxLength);

      for (int roomId = start; roomId <= end; roomId++) {
        String queueName  = "room." + roomId;
        String routingKey = "room." + roomId;
        channel.queueDeclare(queueName, true, false, false, args);
        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
      }
    }
  }
}
