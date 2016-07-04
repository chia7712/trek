package com.spright.trek.task;

import com.spright.trek.DConstants;
import com.spright.trek.exception.TaskIOException;
import com.spright.trek.utils.TrekUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class AccessTaskExecutorFactory {

  private static final Log LOG = LogFactory.getLog(AccessTaskExecutorFactory.class);

  public static AccessTaskExecutor newInstance(final Configuration config, final AccessTaskLogger logger) {
    return new DefaultExecutor(logger,
            config.getInt(DConstants.ACCESS_HANDLER_NUMBER,
                    DConstants.DEFAULT_ACCESS_HANDLER_NUMBER));
  }

  private static class DefaultExecutor implements AccessTaskExecutor {

    private static final int BUFFER_SIZE = 4 * 1024;
    private final AccessTaskLogger logger;
    private final Semaphore available;
    private final ExecutorService service;
    private final Map<String, AccessTask> tasks = new TreeMap<>();

    DefaultExecutor(final AccessTaskLogger logger, final int threadNumber) {
      this.logger = logger;
      this.available = new Semaphore(threadNumber, true);
      this.service = Executors.newFixedThreadPool(threadNumber);
    }

    @Override
    public Optional<AccessTask> find(final String id) {
      synchronized (tasks) {
        return Optional.ofNullable(tasks.get(id));
      }
    }

    @Override
    public AccessTask submit(final AccessTaskRequest request, final TimeUnit unit, final long timeout) {
      AccessTaskImpl task = new AccessTaskImpl(request);
      try {
        if (!available.tryAcquire(timeout, unit)) {
          task.done(new TaskIOException("No available thread"), TaskState.FAILED);
          return task;
        }
      } catch (InterruptedException e) {
        task.done(e, TaskState.FAILED);
        return task;
      }
      synchronized (tasks) {
        tasks.put(task.getStatus().getId(), task);
      }
      service.execute(() -> {
        InputStream input = request.getInput().getInputStream();
        OutputStream output = request.getOutput().getOutputStream();
        byte[] buf = new byte[BUFFER_SIZE];
        int rval;
        int dealtBytes = 0;
        try {
          task.setState(TaskState.RUNNING);
          while ((rval = input.read(buf)) != -1) {
            output.write(buf, 0, rval);
            task.addBytes(rval);
            if (task.isAbort()) {
              task.done(null, TaskState.ABORT);
              return;
            }
            dealtBytes += rval;
            if (dealtBytes >= buf.length) {
              logger.add(task.getStatus());
              dealtBytes -= buf.length;
            }
          }
          task.done(null, TaskState.SUCCEED);
        } catch (IOException e) {
          task.done(e, TaskState.FAILED);
        } finally {
          TrekUtils.closeWithLog(() -> logger.add(task.getStatus()), LOG);
          synchronized (tasks) {
            tasks.remove(task.getStatus().getId());
          }
          available.release();
        }
      });
      return task;
    }

    @Override
    public void close() throws Exception {
      service.shutdownNow();
      service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    }

    @Override
    public AccessTask submit(AccessTaskRequest request) {
      return submit(request, TimeUnit.MILLISECONDS, 0);
    }
  }

  private static class AccessTaskImpl implements AccessTask {

    private final String id;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicLong currentSize = new AtomicLong(0);
    private final long startTime = System.currentTimeMillis();
    private volatile boolean abort = false;
    private volatile TaskState state = TaskState.PENDING;
    private volatile long endTime = 0;
    private volatile Exception ex = null;
    private final AccessTaskRequest request;

    AccessTaskImpl(final AccessTaskRequest request) {
      this.id = createId(startTime);
      this.request = request;
    }

    private static String createId(final long t) {
      return t + "-" + UUID.randomUUID().toString().replaceAll("-", "");
    }

    boolean isAbort() {
      return abort;
    }

    void done(final Exception e, final TaskState state) {
      endTime = System.currentTimeMillis();
      ex = e;
      //Do we need recover change if we are failed to close input??
      ex = TrekUtils.closeWithLog(request.getOutput(), LOG).orElse(ex);
      ex = TrekUtils.closeWithLog(request.getInput(), LOG).orElse(ex);
      if (ex != null) {
        //We recover output data if any error has happened
        ex = TrekUtils.closeWithLog(() -> request.getOutput().recover(), LOG)
                .orElse(ex);
      }
      setState(state);
      latch.countDown();
    }

    void addBytes(final long bytes) {
      currentSize.addAndGet(bytes);
    }

    void setState(TaskState s) {
      state = s;
    }

    @Override
    public void waitCompletion() throws Exception {
      latch.await();
      if (ex != null) {
        throw ex;
      }
    }

    @Override
    public void abort() throws Exception {
      abort = true;
      waitCompletion();
    }

    @Override
    public AccessStatus getStatus() {
      final long transferredSize = currentSize.get();
      final long expectedSize = request.getInput().getInfo().getSize();
      return AccessStatus.newBuilder()
              .setId(id)
              .setRedirectFrom(request.getRedirectFrom())
              .setServerName(request.getServerName())
              .setClientName(request.getClientName())
              .setFrom(request.getInput().getInfo().toString())
              .setTo(request.getOutput().getRequest().toString())
              .setTaskState(state)
              .setProgress((double) transferredSize / (double) expectedSize)
              .setStartTime(startTime)
              .setElapsed(endTime == 0 ? System.currentTimeMillis() - startTime
                      : endTime - startTime)
              .setExpectedSize(expectedSize)
              .setTransferredSize(transferredSize)
              .build();
    }

    @Override
    public AccessTaskRequest getRequest() {
      return request;
    }
  }
}
