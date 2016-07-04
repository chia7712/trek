package com.spright.trek.thread;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Wraps a object to share. The really shared object is released when the
 * reference count has downed to zero. The {@link AutoCloseable#close()} will be
 * invoked if the really shared object is subclass of {@link AutoCloseable}.
 * <p>
 * For example, the class below generates singleton AtomicInteger. The
 * sharedString and sharedStringRef are different instances but they have the
 * same AtomicInteger.
 * <pre>
 * ShareableObject&lt;AtomicInteger&gt; sharedString
 *     = ShareableObject.create(null, () -> new AtomicInteger(1));
 * ShareableObject&lt;AtomicInteger&gt; sharedStringRef
 *     = ShareableObject.create((Class&lt;?&gt; clz, Object obj) -> true,
 *      () -> new AtomicInteger(1));
 * </pre>
 *
 * The CREATE and Close methods are synchronized by lock, so this class is
 * unsuitable to multi-create/multi-close.
 *
 * @param <T> The shared object's type
 */
public final class ShareableObject<T> implements AutoCloseable {

  private static final Log LOG = LogFactory.getLog(ShareableObject.class);
  /**
   * Saves the object and index. The index is incremental, so the error may be
   * happened when the index reachs the {@link Integer#MAX_VALUE}.
   */
  private static final Map<Integer, CountableObject<?>> OBJECTS = new HashMap<>();
  /**
   * Synchronized the collected objects.
   */
  private static final Object GLOBAL_LOCK = new Object();
  /**
   * Indexs the object. It is used for removing the unused object in the
   * {@link HashMap}.
   */
  private static final AtomicInteger OBJECT_INDEX = new AtomicInteger(0);

  public static <T> ShareableObject<T> create(Class<T> clz,
          Supplier<T> supplier) throws Exception {
    return create(clz, null, supplier, (T t) -> {
      try {
        if (AutoCloseable.class.isAssignableFrom(t.getClass())) {
          ((AutoCloseable) t).close();
        }
      } catch (Exception e) {
        LOG.error(e);
      }
    });
  }

  /**
   * Makes the object shareable. If no object is choosed by the {@link Agent} or
   * the {@link Agent} is null, a new object will be created by the
   * {@link Supplier}.
   *
   * @param <T> The shared object's type
   * @param clz
   * @param agent Chooses the object to share
   * @param supplier Creates a object if necessory
   * @param releaser Releases the shared obj when no reference to it
   * @return A object with shareable wrap
   * @throws Exception If failed to create new object
   */
  @SuppressWarnings("unchecked")
  public static <T> ShareableObject<T> create(Class<T> clz, Predicate<T> agent,
          Supplier<T> supplier, final Consumer<T> releaser) throws Exception {
    synchronized (GLOBAL_LOCK) {
      for (CountableObject<?> countObj : OBJECTS.values()) {
        Object obj = countObj.get();
        if (clz != null && clz.isAssignableFrom(obj.getClass())) {
          T castObj = (T) obj;
          if ((agent == null || agent.test(castObj))
                  && countObj.incrementRef()) {
            return new ShareableObject<>((CountableObject<T>) countObj);
          }
        }
      }
      T newObj = supplier.get();
      int index = OBJECT_INDEX.getAndIncrement();
      CountableObject<T> obj = new CountableObject<>(
              index, newObj, releaser);
      OBJECTS.put(index, obj);
      return new ShareableObject<>(obj);
    }
  }

  @FunctionalInterface
  public interface Supplier<T> {

    T get() throws Exception;
  }

  /**
   * @return The number of shared object
   */
  public static int size() {
    synchronized (GLOBAL_LOCK) {
      return OBJECTS.size();
    }
  }
  /**
   * The object to share.
   */
  private final CountableObject<T> obj;
  /**
   * Indicates the {@link ShareableObject#close()} is invoked or not.
   */
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  private ShareableObject(final CountableObject<T> object) {
    obj = object;
  }

  public boolean hasReference() {
    return obj.getReferenceCount() != 0;
  }

  /**
   * @return Current reference count
   */
  public int getReferenceCount() {
    return obj.getReferenceCount();
  }

  /**
   * Checks the internal count.
   *
   * @return True if the internal object is closed
   */
  public boolean isClosed() {
    return isClosed.get();
  }

  /**
   * @return The object to share
   */
  public T get() {
    if (isClosed()) {
      throw new RuntimeException("This outer object is closed");
    }
    return obj.get();
  }

  @Override
  public void close() throws Exception {
    if (isClosed.compareAndSet(false, true)) {
      obj.close();
    }
  }

  private static class CountableObject<T> implements AutoCloseable {

    private final int index;
    private final T object;
    private final Consumer<T> releaser;
    private final AtomicInteger refCount = new AtomicInteger(1);

    CountableObject(final int index, final T object, final Consumer<T> releaser) {
      this.index = index;
      this.object = object;
      this.releaser = releaser;
    }

    int getIndex() {
      return index;
    }

    T get() {
      if (isClosed()) {
        throw new RuntimeException("The inner object is closed");
      }
      return object;
    }

    boolean isClosed() {
      return refCount.get() <= 0;
    }

    int getReferenceCount() {
      return refCount.get();
    }

    boolean incrementRef() {
      return refCount.updateAndGet(x -> x <= 0 ? 0 : x + 1) != 0;
    }

    @Override
    public void close() throws Exception {
      boolean needRelease = false;
      synchronized (GLOBAL_LOCK) {
        if (refCount.decrementAndGet() == 0) {
          OBJECTS.remove(getIndex());
          needRelease = true;
        }
      }
      if (needRelease) {
        releaser.accept(object);
      }
    }
  }
}
