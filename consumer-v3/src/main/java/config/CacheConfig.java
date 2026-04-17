package config;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * In-memory cache for Metrics API results. Uses Caffeine with a 3-minute TTL
 * Analytics queries scan millions of rows, caching avoids re-running expensive aggregations on repeated calls.
 * Cache is invalidated automatically after TTL expires.
 */
@Configuration
@EnableCaching
public class CacheConfig {

  @Bean
  public CacheManager cacheManager() {
    CaffeineCacheManager manager = new CaffeineCacheManager(
        "messageStats", // messages/sec, total, duration
        "topUsers", // top N users by message count
        "topRooms", // top N rooms by message count
        "participationPatterns" // user participation patterns
    );
    manager.setCaffeine(Caffeine.newBuilder()
        .expireAfterWrite(3, TimeUnit.MINUTES)
        .maximumSize(20));
    return manager;
  }
}
