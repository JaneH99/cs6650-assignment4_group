package config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

/**
 * Configures two DataSource beans for sharded database architecture:
 *   DataSource-1 → rooms 1-10 (db1.c8p0g0yoy776.us-east-1.rds.amazonaws.com)
 *   DataSource-2 → rooms 11-20 (db2.c8p0g0yoy776.us-east-1.rds.amazonaws.com)
 */
@Configuration
public class DataSourceConfig {

  @Value("${spring.datasource.url}")
  private String db1Url;

  @Value("${spring.datasource.username}")
  private String db1Username;

  @Value("${spring.datasource.password}")
  private String db1Password;

  @Value("${spring.datasource2.url}")
  private String db2Url;

  @Value("${spring.datasource2.username}")
  private String db2Username;

  @Value("${spring.datasource2.password}")
  private String db2Password;

  /**
   * Primary DataSource for rooms 1-10.
   * Used as the default DataSource by JdbcTemplate unless explicitly specified.
   */
  @Bean
  @Primary
  public DataSource dataSource1() {
    return DataSourceBuilder.create()
        .url(db1Url)
        .username(db1Username)
        .password(db1Password)
        .driverClassName("org.postgresql.Driver")
        .build();
  }

  /**
   * Secondary DataSource for rooms 11-20.
   * Accessed via DataSourceHolder.getDataSource2().
   */
  @Bean
  public DataSource dataSource2() {
    return DataSourceBuilder.create()
        .url(db2Url)
        .username(db2Username)
        .password(db2Password)
        .driverClassName("org.postgresql.Driver")
        .build();
  }
}
