package db;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.context.SmartLifecycle;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * Periodically refreshes the three analytics materialized views (analytics query 2 to 4) using REFRESH CONCURRENTLY
 * (readers are never blocked during refresh). After each refresh the corresponding Caffeine
 * cache entries are evicted so the next read picks up fresh MV data.
 *
 * Phase = MAX_VALUE - 2: starts before DatabaseWriter, stops after DatabaseWriter — final refresh runs after
 * all DB writes have drained.
 */
@Component
public class MaterializedViewRefresher implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(MaterializedViewRefresher.class);

  static final String[] VIEWS = {
      "mv_top_users",
      "mv_top_rooms",
      "mv_user_participation"
  };

  private static final String[] CACHES = {
      "topUsers",
      "topRooms",
      "participationPatterns"
  };

  private final JdbcTemplate jdbc;
  private final CacheManager cacheManager;
  private final long refreshIntervalSec;

  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "mv-refresher"));
  private volatile boolean running = false;

  public MaterializedViewRefresher(
      JdbcTemplate jdbc,
      CacheManager cacheManager,
      @Value("${mv.refresh.interval-sec:30}") long refreshIntervalSec) {
    this.jdbc = jdbc;
    this.cacheManager = cacheManager;
    this.refreshIntervalSec = refreshIntervalSec;
  }

  @Override
  public void start() {
    running = true;
    scheduler.scheduleAtFixedRate(
        this::refreshAll, refreshIntervalSec, refreshIntervalSec, TimeUnit.SECONDS);
    log.info("MaterializedViewRefresher started: interval={}s", refreshIntervalSec);
  }

  /**
   * Refreshes all analytics MVs concurrently then evicts the matching Caffeine caches.
   * Called on the scheduled interval
   */
  public void refreshAll() {
    for (String view : VIEWS) {
      try {
        long t0 = System.currentTimeMillis();
        jdbc.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY " + view);
        log.info("[MV REFRESH] {} refreshed in {}ms", view, System.currentTimeMillis() - t0);
      } catch (Exception e) {
        log.warn("[MV REFRESH] Failed to refresh {}: {}", view, e.getMessage());
      }
    }
    evictCaches();
  }

  /**
   * Non-concurrent refresh — used after TRUNCATE when the table is empty and
   * CONCURRENTLY is unnecessary overhead. called by MetricsRepository.truncate().
   */
  public void refreshAllDirect() {
    for (String view : VIEWS) {
      try {
        jdbc.execute("REFRESH MATERIALIZED VIEW " + view);
        log.debug("[MV REFRESH] {} refreshed (direct)", view);
      } catch (Exception e) {
        log.warn("[MV REFRESH] Failed to refresh (direct) {}: {}", view, e.getMessage());
      }
    }
    evictCaches();
  }

  // Evict cache so subsequent queries read fresh data from refreshed materialized views
  private void evictCaches() {
    for (String name : CACHES) {
      org.springframework.cache.Cache cache = cacheManager.getCache(name);
      if (cache != null) cache.clear();
    }
    log.debug("[MV REFRESH] Analytics caches evicted");
  }

  @Override
  public void stop() {
    running = false;
    scheduler.shutdownNow();
    log.info("MaterializedViewRefresher stopped");
  }

  @Override public boolean isRunning() { return running; }
  @Override public int getPhase() { return Integer.MAX_VALUE - 2; }
}