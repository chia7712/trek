package com.spright.trek.task;

import com.spright.trek.query.CloseableIterator;
import java.io.IOException;
import java.util.Optional;

/**
 * Logs the completed task.
 */
public interface AccessTaskLogger extends AutoCloseable {

  boolean supportOrder(AccessStatusQuery query);

  boolean supportFilter(final AccessStatusQuery query);

  void add(AccessStatus status) throws IOException;

  Optional<AccessStatus> find(final String id) throws IOException;

  CloseableIterator<AccessStatus> list(final AccessStatusQuery query) throws IOException;

  CloseableIterator<AccessStatus> list() throws IOException;
}
