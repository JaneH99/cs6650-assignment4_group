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
 * Configures a single RabbitMQ ChannelPool and declares topology.
 *
 * Producer connects to ONE RabbitMQ instance:
 *   ChannelPool → all rooms 1-20
 *
 * Topology (declared on startup):
 *   Exchange: chat.exchange  (TOPIC, durable)
 *   Queues: room.1 … room.20 (durable, x-message-ttl, x-max-length)
 *   Bindings: room.i → chat.exchange with routing key "room.i"
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

  @Value("${rabbitmq.channel.pool.size:20}")
  private int poolSize;

  @Value("${rabbitmq.rooms:20}")
  private int numRooms;

  @Value("${rabbitmq.queue.message-ttl:60000}")
  private int messageTtl;

  @Value("${rabbitmq.queue.max-length:10000}")
  private int maxLength;

  @Bean(destroyMethod = "closeAll")
  public ChannelPool channelPool() throws Exception {
    ChannelPool pool = new ChannelPool(host, username, password, poolSize);
    declareTopology(pool);
    return pool;
  }

  private void declareTopology(ChannelPool pool) throws Exception {
    Channel channel = pool.borrowChannel();
    try {
      channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC, true);

      Map<String, Object> args = new HashMap<>();
      args.put("x-message-ttl", messageTtl);
      args.put("x-max-length", maxLength);

      for (int roomId = 1; roomId <= numRooms; roomId++) {
        String queueName  = "room." + roomId;
        String routingKey = "room." + roomId;
        channel.queueDeclare(queueName, true, false, false, args);
        channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
      }
    } finally {
      pool.returnChannel(channel);
    }
  }
}
