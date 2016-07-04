package com.spright.trek.thread;

import com.spright.trek.io.AtomicCloseable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javafx.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A restricted map is implemented by {@link ArrayList}. It invokes a thread to
 * check all {
 *
 * @like Value} for deleting idle one. It need a {@link ValueBuilder} to build {
 * @like Value} if there are enough room to add new element.
 * @param <N> {@link Name}
 * @param <V> {@link Value}
 */
public final class RestrictedListMap<
    N extends Comparable<N>, V extends RestrictedListMap.Value>
        extends AtomicCloseable {

  /**
   * Element status.
   */
  public enum Status {
    /**
     * The elements is free.
     */
    FREE,
    /**
     * The elements is busy.
     */
    BUSY,
    /**
     * The elements is dead. It cannot be used anymore.
     */
    DEAD
  }
  /**
   * Log.
   */
  private static final Log LOG = LogFactory.getLog(RestrictedListMap.class);
  /**
   * Max size.
   */
  private final int maxSize;
  /**
   * Lock for prevent collection of list operation.
   */
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  /**
   * List to save elements.
   */
  private final List<Pair<N, V>> elements;
  /**
   * Thread pool. Only one thread will be invoked.
   */
  private final ExecutorService service = Executors.newSingleThreadExecutor();
  /**
   * The period to check the timout elements.
   */
  private final long checkTime;
  /**
   * True if there are one running check thread.
   */
  private final AtomicBoolean hasCheckThread = new AtomicBoolean(false);

  /**
   * Constructs a {@link RestrictedListMap} with specified size, builder and
   * check period.
   *
   * @param maxSize Max size of elements saved in {@link RestrictedListMap}
   * @param checkTime The period(second) to check idle value
   */
  public RestrictedListMap(final int maxSize, final int checkTime) {
    this.maxSize = maxSize;
    this.elements = new ArrayList<>(maxSize);
    this.checkTime = checkTime;
  }

  /**
   * Closes the timeout elements.
   *
   * @return The number of available elements
   */
  private int checkTimeoutElements() {
    List<Pair<N, V>> available = new ArrayList<>(elements.size());
    elements.forEach(e -> {
      //Close the timeout element
      if (e.getValue().isTimeout()) {
        try {
          e.getValue().close();
        } catch (Exception ex) {
          LOG.error("Failed to close object", ex);
        }
      }
      //collect the alive element
      if (!e.getValue().isDead()) {
        available.add(e);
      }
    });
    elements.clear();
    elements.addAll(available);
    return elements.size();
  }

  /**
   * Returns the number of elements in this list. If this list contains more
   * than <tt>Integer.MAX_VALUE</tt> elements, returns
   * <tt>Integer.MAX_VALUE</tt>.
   *
   * @return the number of elements in this list
   */
  public int size() {
    lock.writeLock().lock();
    try {
      return checkTimeoutElements();
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Invoked after {@link RestrictedListMap#get(Comparable, ValueBuilder)} has
   * been called. An new check thread will be constructed if check threas isn't
   * existed. It is required to limit only one thread for avoiding too many
   * write lock which lower the performance.
   */
  private void invokeCheckThread() {
    if (hasCheckThread.compareAndSet(false, true)) {
      service.execute(() -> {
        try {
          while (!Thread.interrupted()) {
            TimeUnit.SECONDS.sleep(checkTime);
            lock.writeLock().lock();
            try {
              if (checkTimeoutElements() == 0) {
                return;
              }
            } finally {
              lock.writeLock().unlock();
            }
          }
        } catch (InterruptedException e) {
          LOG.info(getClass().getName() + " is closeing");
        } finally {
          hasCheckThread.set(false);
        }
      });
    }
  }

  /**
   * Gets the free element, otherwise a new elemenet will return.
   *
   * @param name Name
   * @param builder Value builder
   * @return A element
   * @throws IOException If there are no enough room to save new element
   */
  public V get(final N name, final ValueBuilder<V> builder)
          throws IOException {
    checkClose();
    lock.readLock().lock();
    try {
      for (int i = 0; i != elements.size(); ++i) {
        Pair<N, V> e = elements.get(i);
        if (e.getKey().compareTo(name) == 0
                && e.getValue().checkFreeAndSetBusy()) {
          invokeCheckThread();
          return e.getValue();
        }
      }
    } finally {
      lock.readLock().unlock();
    }
    lock.writeLock().lock();
    try {
      if (elements.size() > maxSize) {
        throw new IOException(
                "No available resource, max size is " + maxSize);
      }
      V newOne = builder.build();
      elements.add(new Pair<>(name, newOne));
      newOne.checkFreeAndSetBusy();
      invokeCheckThread();
      return newOne;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void internalClose() throws IOException {
    service.shutdown();
    try {
      service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * Builder for creating new {@link Value}.
   *
   * @param <V> Value
   */
  @FunctionalInterface
  public interface ValueBuilder<V extends RestrictedListMap.Value> {

    /**
     * @return A new value
     * @throws IOException if failed to build value
     */
    V build() throws IOException;
  }

  /**
   * Name with comparable interface. It is used for find the specified
   * {@link Value}
   */
//    public interface Name extends Comparable<Name> {
//    }
  /**
   * User defined.
   */
  public abstract static class Value implements AutoCloseable {

    /**
     * Current time.
     */
    private long currentTime = System.currentTimeMillis();
    /**
     * Using flag.
     */
    private final AtomicReference<Status> status
            = new AtomicReference<>(Status.FREE);
    /**
     * The time to idle.
     */
    private final long idlePeriod;

    /**
     * @param idleTime The max time to idle (millisecond)
     */
    public Value(final long idleTime) {
      idlePeriod = idleTime;
    }

    /**
     * @return True if the value is free now
     */
    final boolean checkFreeAndSetBusy() {
      return status.compareAndSet(Status.FREE, Status.BUSY);
    }

    /**
     * Free this object. If the status of object is dead, the runtime exception
     * will happen.
     */
    public final void free() {
      if (!status.compareAndSet(Status.BUSY, Status.FREE)) {
        throw new RuntimeException("Expect to change status from"
                + " busy to free");
      }
      currentTime = System.currentTimeMillis();
    }

    /**
     * @return true if object is dead
     */
    final boolean isDead() {
      return status.get() == Status.DEAD;
    }

    /**
     * @return true if this value should be closed
     */
    final boolean isTimeout() {
      return status.get() == Status.FREE
              && ((System.currentTimeMillis() - currentTime) >= idlePeriod);
    }

    /**
     * Releases the resource within object.
     *
     * @throws IOException If failed to release
     */
    protected abstract void clean() throws IOException;

    @Override
    public final void close() throws Exception {
      if (status.getAndSet(Status.DEAD) != Status.DEAD) {
        clean();
      }
    }
  }
}
