package websocket;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import jakarta.annotation.PreDestroy;
import model.UserInfo;

/**
 * This class is a Spring-managed singleton bean and is initialized at application startup.
 * It adds and removes sessions to roomSessions, it tracks active users, and in charge of message broadcasting.
 * roomSessions:  roomId    → Set<WebSocketSession> (for broadcast)
 * activeUsers:   sessionId → UserInfo              (for metrics / health)
 * roomExecutors: roomId    → single-threaded ExecutorService (preserves per-room message order)
 */
@Component
public class SessionRegistry {

  private static final Logger log = LoggerFactory.getLogger(SessionRegistry.class);

//  For current load test, one room is mapped to one websocket. But for real application, one room will be mapped to different ws, each ws is associated one client
  private final ConcurrentHashMap<String, Set<WebSocketSession>> roomSessions =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, UserInfo> activeUsers =
      new ConcurrentHashMap<>();

  // One single-threaded executor per room:
  //   - guarantees FIFO broadcast order within each room
  //   - different rooms broadcast in parallel
  // This design won't help current load test since one ws is for one room in load test. But for real application, where each ws is for each client, it will help
  private final ConcurrentHashMap<String, ExecutorService> roomExecutors =
      new ConcurrentHashMap<>();

  public void register(String roomId, WebSocketSession session, String userId, String username) {
    roomSessions.computeIfAbsent(roomId, k -> ConcurrentHashMap.newKeySet()).add(session);
    activeUsers.put(session.getId(), new UserInfo(userId, username, session.getId()));
    log.debug("Session registered: sessionId={} user={}", session.getId(), userId);
  }

  public void unregister(String roomId, WebSocketSession session) {
    Set<WebSocketSession> sessions = roomSessions.get(roomId);
    if (sessions != null) {
      sessions.remove(session);
    }
    activeUsers.remove(session.getId());
    log.debug("Session unregistered: sessionId={} room={}", session.getId(), roomId);
  }

  /**
   * Submits a broadcast task to the room's dedicated single-threaded executor.
   * The executor is created lazily on first use for that room.
   * Because each room's executor is single-threaded, messages are sent in the
   * order they are submitted.
   * Different rooms broadcast concurrently via their own executors.
   */
  public void broadcast(String roomId, String json) {
    ExecutorService executor = roomExecutors.computeIfAbsent(
        roomId, k -> Executors.newSingleThreadExecutor(
            r -> new Thread(r, "broadcast-room-" + k)));
    executor.execute(() -> sendToRoom(roomId, json));
  }

  private void sendToRoom(String roomId, String json) {
    Set<WebSocketSession> sessions = roomSessions.getOrDefault(roomId, Collections.emptySet());
    if (sessions.isEmpty()) return;

    TextMessage message = new TextMessage(json);
    for (WebSocketSession session : sessions) {
      if (!session.isOpen()) continue;
      synchronized (session) {
        try {
          synchronized (session) {
            if (session.isOpen()) {
              session.sendMessage(message);
            }
          }
        } catch (IllegalStateException e) {
          // Thrown when the remote endpoint is mid-send (TEXT_PARTIAL_WRITING).
          // Log and drop — the client will reconnect if needed.
          log.warn("Session {} in invalid state during send, skipping: {}",
              session.getId(), e.getMessage());
        } catch (IOException e) {
          log.warn("Failed to send to session {}: {}", session.getId(), e.getMessage());
        }
      }
    }
  }

  @PreDestroy
  public void shutdown() {
    roomExecutors.values().forEach(ExecutorService::shutdown);
    roomExecutors.values().forEach(executor -> {
      try {
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    });
  }

  public int getActiveUsersCount() {
    return activeUsers.size();
  }

  public Object getRoomCount() {
    return roomSessions.mappingCount();
  }
}
