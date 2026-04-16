package config;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import java.util.HashMap;
import java.util.Map;
import mq.ChannelPool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures the RabbitMQ ChannelPool and declares topology.
 *
 * Topology
 *   Exchange: chat.exchange  (TOPIC, durable)
 *   Queues: room.{id}.p.{n}  (durable, x-message-ttl, x-max-length)
 *   Bindings: room.{id}.p.{n} → chat.exchange with routing key "room.{id}.p.{n}"
 *
 * Queue partitioning optimization:
 *   Each room is split into partitionsPerRoom queues.
 *   Messages are routed by hash(userId) % partitionsPerRoom so the same
 *   user always lands on the same partition, and multiple partitions
 *   can be consumed in parallel to increase throughput for hot rooms.
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

  // Pool size should cover all partitions across all rooms: numRooms * partitionsPerRoom
  @Value("${rabbitmq.channel.pool.size:60}")
  private int poolSize;

  @Value("${rabbitmq.rooms:20}")
  private int numRooms;

  // Number of partitions per room — controls parallelism on the consumer side
  @Value("${rabbitmq.partitions.per.room:3}")
  private int partitionsPerRoom;

  @Value("${rabbitmq.queue.message-ttl:60000}")
  private int messageTtl;

  @Value("${rabbitmq.queue.max-length:10000}")
  private int maxLength;

  @Bean(destroyMethod = "closeAll")
  public ChannelPool channelPool() throws Exception {
    ChannelPool pool = new ChannelPool(host, username, password, poolSize);

    Channel channel = pool.borrowChannel();
    try {
      channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true);

      Map<String, Object> args = new HashMap<>();
      args.put("x-message-ttl", messageTtl);
      args.put("x-max-length", maxLength);

      // Each room gets partitionsPerRoom queues instead of one.
      // Total queues = numRooms * partitionsPerRoom (e.g. 20 * 3 = 60)
      for (int roomId = 1; roomId <= numRooms; roomId++) {
        for (int p = 0; p < partitionsPerRoom; p++) {
          String queueName  = "room." + roomId + ".p." + p;
          String routingKey = "room." + roomId + ".p." + p;
          channel.queueDeclare(queueName, true, false, false, args);
          channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        }
      }
    } finally {
      pool.returnChannel(channel);
    }
    return pool;
  }
}