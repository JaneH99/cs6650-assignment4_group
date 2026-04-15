package db;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import model.BroadcastMessage;

/**
 * Shared in-memory buffer for messages waiting to be written to DB.
 * Backed by a bounded blocking queue — if full, offer() returns false
 * and the message is dropped.
 */
@Component
public class MessageBuffer {

    private final LinkedBlockingQueue<BroadcastMessage> queue;
    private final String name;

    public MessageBuffer() {
        this.queue = new LinkedBlockingQueue<>(5000); 
        this.name = "default-buffer";
    }

    public MessageBuffer(@Value("${db.writer.buffer-capacity}") int capacity) {
        this.queue = new LinkedBlockingQueue<>(capacity); 
        this.name = "shared-buffer";
    }

    public MessageBuffer(int capacity, String name) {
        this.queue = new LinkedBlockingQueue<>(capacity);
        this.name = name;
    }

    public boolean offer(BroadcastMessage message) {
        return queue.offer(message);
    }

    public boolean offer(BroadcastMessage message, long timeout, TimeUnit unit)
            throws InterruptedException {
        return queue.offer(message, timeout, unit);
    }

    public BroadcastMessage poll(long timeoutMs, TimeUnit unit)
            throws InterruptedException {
        return queue.poll(timeoutMs, unit);
    }

    public int drainTo(List<BroadcastMessage> target, int maxElements) {
        return queue.drainTo(target, maxElements);
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }

    public String getName() {
        return name;
    }
}