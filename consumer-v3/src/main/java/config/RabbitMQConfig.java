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
 * Topology is declared idempotently on startup.
 * Both consumer and producer declare the same exchange/queues with the same args,
 * so whichever starts first wins; the second is a no-op.
 *
 * Queue partitioning optimization:
 *   Each room is split into partitionsPerRoom queues named "room.{id}.p.{n}".
 *   This allows multiple consumer channels to process one room in parallel,
 *   increasing throughput for hot rooms.
 */
@Configuration
public class RabbitMQConfig {

  public static final String EXCHANGE_NAME = "chat.exchange";

  @Value("${spring.rabbitmq.host}")
  private String host;

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

  @Value("${rabbitmq.queue.message-ttl:60000}")
  private int messageTtl;

  @Value("${rabbitmq.queue.max-length:10000}")
  private int maxLength;

  @Value("${rabbitmq.consumer.threads:4}")
  private int consumerThreads;

  @Value("${rabbitmq.consumer.prefetch:20}")
  private int prefetchCount;

  // Must match the value in producer RabbitMQConfig and MessagePublisher
  @Value("${rabbitmq.partitions.per.room:3}")
  private int partitionsPerRoom;

  @Bean
  public RoomConsumer roomConsumer(MessageProcessingService processingService) throws Exception {
    declareTopology();
    return new RoomConsumer(host, username, password,
            consumerThreads, prefetchCount, numRooms,
            roomsStart, roomsEnd,
            partitionsPerRoom,
            processingService);
  }

  private void declareTopology() throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(host);
    factory.setUsername(username);
    factory.setPassword(password);

    try (Connection conn = factory.newConnection("topology-init");
         Channel channel = conn.createChannel()) {

      channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true);

      Map<String, Object> args = new HashMap<>();
      args.put("x-message-ttl", messageTtl);
      args.put("x-max-length", maxLength);

      // Each room gets partitionsPerRoom queues instead of one.
      // Total queues = numRooms * partitionsPerRoom (e.g. 20 * 3 = 60)
      for (int roomId = roomsStart; roomId <= roomsEnd; roomId++) {
        for (int p = 0; p < partitionsPerRoom; p++) {
          String queueName  = "room." + roomId + ".p." + p;
          String routingKey = "room." + roomId + ".p." + p;
          channel.queueDeclare(queueName, true, false, false, args);
          channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }
      }
    }
  }
}