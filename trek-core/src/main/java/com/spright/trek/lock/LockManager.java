package com.spright.trek.lock;

import java.io.IOException;
import java.util.Optional;

public interface LockManager {

  Lock getReadLock(final String key) throws IOException;

  Lock getWriteLock(final String key) throws IOException;

  Optional<Lock> tryReadLock(final String key) throws IOException;

  Optional<Lock> tryWriteLock(final String key) throws IOException;
}
