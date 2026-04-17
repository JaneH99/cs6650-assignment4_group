package config;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import java.util.HashMap;
import java.util.Map;
import mq.ChannelPool;
import mq.DualChannelPool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures two RabbitMQ ChannelPools and declares topology.
 *
 * Producer connects to TWO RabbitMQ instances:
 *   ChannelPool-1 → rooms 1-10 (host)
 *   ChannelPool-2 → rooms 11-20 (host2)
 *
 * Topology (declared on both):
 *   Exchange: chat.exchange  (TOPIC, durable)
 *   Queues: room.1 … room.20 (durable, x-message-ttl, x-max-length)
 *   Bindings: room.i → chat.exchange with routing key "room.i"
 */
@Configuration
public class RabbitMQConfig {

  public static final String EXCHANGE_NAME = "chat.exchange";

  @Value("${spring.rabbitmq.host}")
  private String host;

  @Value("${spring.rabbitmq2.host:${spring.rabbitmq.host}}")
  private String host2;

  @Value("${spring.rabbitmq.username:guest}")
  private String username;

  @Value("${spring.rabbitmq.password:guest}")
  private String password;

  @Value("${rabbitmq.channel.pool.size:20}")
  private int poolSize;

  @Value("${rabbitmq.channel2.pool.size:20}")
  private int poolSize2;

  @Value("${rabbitmq.rooms:20}")
  private int numRooms;

  @Value("${rabbitmq.queue.message-ttl:60000}")
  private int messageTtl;

  @Value("${rabbitmq.queue.max-length:10000}")
  private int maxLength;

  @Bean(destroyMethod = "closeAll")
  public DualChannelPool channelPool() throws Exception {
    ChannelPool pool1 = new ChannelPool(host, username, password, poolSize);
    ChannelPool pool2 = new ChannelPool(host2, username, password, poolSize2);
    declareTopology(pool1);
    declareTopology(pool2);
    return new DualChannelPool(pool1, pool2, numRooms / 2);
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
