package config;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * In-memory cache for analytics query results. Uses Caffeine with a configurable TTL (default 60s).
 * On cache miss, queries the pre-aggregated materialized views instead of scanning the messages table.
 * TTL must be >= mv.refresh.interval-sec so cached data is evicted after each MV refresh cycle.
 */
@Configuration
@EnableCaching
public class CacheConfig {

  @Bean
  public CacheManager cacheManager(@Value("${cache.ttl-sec:60}") long ttlSec) {
    CaffeineCacheManager manager = new CaffeineCacheManager(
        "messageStats", // messages/sec, total, duration
        "topUsers", // top N users by message count
        "topRooms", // top N rooms by message count
        "participationPatterns" // user participation patterns
    );
    manager.setCaffeine(Caffeine.newBuilder()
        .expireAfterWrite(ttlSec, TimeUnit.SECONDS)
        .maximumSize(20));
    return manager;
  }
}
