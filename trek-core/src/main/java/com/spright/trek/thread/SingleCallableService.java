package com.spright.trek.thread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SingleCallableService implements AutoCloseable {

  private final ExecutorService service = Executors.newSingleThreadExecutor();
  private final AtomicBoolean working = new AtomicBoolean(false);
  private final Runnable worker;

  public SingleCallableService(final Runnable worker) {
    this.worker = worker;
  }

  public void invokeIfAbsent() {
    if (working.compareAndSet(false, true)) {
      service.execute(() -> {
        try {
          worker.run();
        } finally {
          working.set(false);
        }
      });
    }
  }

  @Override
  public void close() throws Exception {
    service.shutdownNow();
    service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
  }

}
