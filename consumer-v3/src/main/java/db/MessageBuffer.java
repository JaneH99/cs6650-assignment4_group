package db;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import model.BroadcastMessage;

/**
 * Shared in-memory buffer between consumer threads and DB writer threads.
 * Backed by a bounded blocking queue — if full, offer() returns false
 * and the message is dropped.
 */
public class MessageBuffer {

  private final LinkedBlockingQueue<BroadcastMessage> queue;

  public MessageBuffer(int capacity) {
    this.queue = new LinkedBlockingQueue<>(capacity);
  }

  public boolean offer(BroadcastMessage message) {
    return queue.offer(message);
  }

  public boolean offer(BroadcastMessage message, long timeout, java.util.concurrent.TimeUnit unit)
      throws InterruptedException {
    return queue.offer(message, timeout, unit);
  }

//  periodic polling with timeout, it is used to poll the first msg per batch
  public BroadcastMessage poll(long timeoutMs, java.util.concurrent.TimeUnit unit)
      throws InterruptedException {
    return queue.poll(timeoutMs, unit);
  }

//  Removes up to maxElements messages from queue to target
  public int drainTo(List<BroadcastMessage> target, int maxElements) {
    return queue.drainTo(target, maxElements);
  }

  public boolean isEmpty() {
    return queue.isEmpty();
  }

  public int size() {
    return queue.size();
  }
}
