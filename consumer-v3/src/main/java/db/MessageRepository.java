package db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import model.BroadcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class MessageRepository {

  private static final Logger log = LoggerFactory.getLogger(MessageRepository.class);

//  SQL command to insert message into messages table.
//  ON CONFLICT (message_id) DO NOTHING prevent dup message to be written into DB, although it should have already been checked before DB write
  private static final String SQL =
      "INSERT INTO messages (message_id, room_id, user_id, timestamp, content) " +
      "VALUES (?, ?, ?, ?, ?) ON CONFLICT (message_id) DO NOTHING";

  private final JdbcTemplate jdbc;

  public MessageRepository(JdbcTemplate jdbc) {
    this.jdbc = jdbc;
  }

  /**
   * Inserts a list of BroadcastMessage objects into the database in a batch.
   * @param msgs List of BroadcastMessage to insert
   */
  @Transactional
  public void batchInsert(List<BroadcastMessage> msgs) {
    jdbc.batchUpdate(SQL, msgs, msgs.size(), new ParameterizedPreparedStatementSetter<BroadcastMessage>() {
      @Override
      public void setValues(PreparedStatement ps, BroadcastMessage msg) throws SQLException {
        ps.setString(1, msg.getMessageId());
        ps.setString(2, msg.getRoomId());
        ps.setString(3, msg.getUserId());
        ps.setString(4, msg.getTimestamp());
        ps.setString(5, msg.getMessage());
      }
    });
    log.debug("Inserted {} messages", msgs.size());
  }
}
