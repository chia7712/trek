package com.spright.trek.task;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public interface AccessTaskExecutor extends AutoCloseable {

  AccessTask submit(final AccessTaskRequest request);

  AccessTask submit(final AccessTaskRequest request, final TimeUnit unit, final long timeout);

  Optional<AccessTask> find(final String id);
}
