package db;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * Manages DataSource routing for sharded database architecture.
 * ThreadLocal holds the current room's DataSource context during batch processing.
 * Provides static access to both DataSource instances.
 */
@Component
public class DataSourceHolder {

  private final DataSource dataSource1;
  private final DataSource dataSource2;

  private static final ThreadLocal<DataSource> contextHolder = new ThreadLocal<>();

  @Autowired
  public DataSourceHolder(DataSource dataSource1, DataSource dataSource2) {
    this.dataSource1 = dataSource1;
    this.dataSource2 = dataSource2;
  }

  /**
   * Sets the DataSource context for the current thread based on roomId.
   * Rooms 1-10 → DataSource-1
   * Rooms 11-20 → DataSource-2
   */
  public static void setDataSource(int roomId) {
    if (roomId >= 1 && roomId <= 10) {
      contextHolder.set(getInstance().dataSource1);
    } else {
      contextHolder.set(getInstance().dataSource2);
    }
  }

  /**
   * Clears the DataSource context for the current thread.
   */
  public static void clear() {
    contextHolder.remove();
  }

  /**
   * Gets the DataSource for the current thread's context.
   * Falls back to DataSource-1 if no context is set.
   */
  public static DataSource getCurrent() {
    DataSource ds = contextHolder.get();
    return ds != null ? ds : getInstance().dataSource1;
  }

  public DataSource getDataSource1() {
    return dataSource1;
  }

  public DataSource getDataSource2() {
    return dataSource2;
  }

  private static DataSourceHolder instance;

  @Autowired
  private void init(DataSource dataSource1, DataSource dataSource2) {
    instance = this;
  }

  private static DataSourceHolder getInstance() {
    if (instance == null) {
      throw new IllegalStateException("DataSourceHolder not initialized");
    }
    return instance;
  }
}
