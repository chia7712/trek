package com.spright.trek.io;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Atomic-clseable class. It guarantee the close method is invoked only once.
 */
public abstract class AtomicCloseable implements AutoCloseable {

  /**
   * Close flag.
   */
  private final AtomicBoolean isClosed = new AtomicBoolean(false);

  /**
   * @return True if is closed
   */
  public final boolean isClosed() {
    return isClosed.get();
  }

  /**
   * Internal close to inherit.
   *
   * @throws IOException If any error happen
   */
  protected abstract void internalClose() throws Exception;

  @Override
  public final void close() throws Exception {
    if (isClosed.compareAndSet(false, true)) {
      internalClose();
    }
  }

  protected void checkClose() throws IOException {
    if (isClosed()) {
      throw new IOException(getClass().getName() + " is closed");
    }
  }
}
