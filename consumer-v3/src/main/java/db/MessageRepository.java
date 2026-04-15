package db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import model.BroadcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;

/**
 * Repository for batch inserting messages into the sharded database.
 * Each instance is bound to a specific DataSource (DB1 or DB2).
 */
@Repository
public class MessageRepository {

  private static final Logger log = LoggerFactory.getLogger(MessageRepository.class);

  private static final String SQL =
      "INSERT INTO messages (message_id, room_id, user_id, timestamp, content) " +
      "VALUES (?, ?, ?, ?, ?) ON CONFLICT (message_id) DO NOTHING";

  private final JdbcTemplate jdbcTemplate;

  /**
   * Creates a MessageRepository bound to the specified DataSource.
   * Used by DatabaseWriter to write to either DB1 or DB2.
   */
  public MessageRepository(DataSource dataSource) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
  }

  /**
   * Inserts a list of BroadcastMessage objects into the database in a batch.
   * @param msgs List of BroadcastMessage to insert
   */
  public void batchInsert(List<BroadcastMessage> msgs) {
    if (msgs == null || msgs.isEmpty()) {
      return;
    }
    jdbcTemplate.batchUpdate(SQL, msgs, msgs.size(), new ParameterizedPreparedStatementSetter<BroadcastMessage>() {
      @Override
      public void setValues(PreparedStatement ps, BroadcastMessage msg) throws SQLException {
        ps.setString(1, msg.getMessageId());
        ps.setString(2, msg.getRoomId());
        ps.setString(3, msg.getUserId());
        ps.setString(4, msg.getTimestamp());
        ps.setString(5, msg.getMessage());
      }
    });
    log.debug("Inserted {} messages to DB", msgs.size());
  }

  /**
   * Truncates the messages table.
   * Used for testing purposes.
   */
  public void truncate() {
    jdbcTemplate.execute("TRUNCATE TABLE messages");
  }
}
