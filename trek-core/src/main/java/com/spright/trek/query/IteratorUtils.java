package com.spright.trek.query;

import com.spright.trek.utils.TrekUtils;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;

public final class IteratorUtils {

  /**
   * The empty iterator (immutable).
   */
  @SuppressWarnings("rawtypes")
  private static final CloseableIterator EMPTY_LIST = new CloseableIterator() {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    public Object next() {
      throw new NoSuchElementException();
    }

    @Override
    public void close() throws IOException {
    }
  };

  @SuppressWarnings("unchecked")
  public static final <T> CloseableIterator<T> empty() {
    return (CloseableIterator<T>) EMPTY_LIST;
  }

  public static <T> CloseableIterator<T> wrap(final Iterator<T> iter,
          final Closeable closeable) {
    return new CloseableIterator<T>() {
      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public T next() {
        return iter.next();
      }

      @Override
      public void close() throws IOException {
        if (closeable != null) {
          closeable.close();
        }
      }
    };
  }

  public static <T> CloseableIterator<T> wrap(final Iterator<T> iter) {
    return wrap(iter, () -> {
    });
  }

  public static <T> CloseableIterator<T> wrap(final T init) {
    return wrap(new Iterator<T>() {
      private T current = init;

      @Override
      public boolean hasNext() {
        return current != null;
      }

      @Override
      public T next() {
        if (current == null) {
          throw new NoSuchElementException();
        }
        try {
          return current;
        } finally {
          current = null;
        }
      }
    });
  }

  public static <T> CloseableIterator<T> wrap(final Iterator<T> iter,
          final Function<List<T>, List<T>> function, final int batch) {
    return wrap(wrap(iter), function, batch);
  }

  public static <T> CloseableIterator<T> wrap(final CloseableIterator<T> iter,
          final Function<List<T>, List<T>> function, final int batch) {
    if (batch <= 0) {
      throw new IllegalArgumentException("The batch size must be bigger than zero");
    }
    return new CloseableIterator<T>() {
      private final List<T> buffered = new ArrayList<>(batch);
      private final List<T> rval = new ArrayList<>(batch);

      @Override
      public boolean hasNext() {
        while (true) {
          if (!rval.isEmpty()) {
            return true;
          }
          if (iter.hasNext()) {
            buffered.add(iter.next());
            if (buffered.size() >= batch) {
              rval.addAll(function.apply(buffered));
              buffered.clear();
            }
          } else {
            if (buffered.isEmpty()) {
              return false;
            }
            rval.addAll(function.apply(buffered));
            buffered.clear();
          }
        }
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        return rval.remove(0);
      }

      @Override
      public void close() throws IOException {
        iter.close();
      }
    };
  }

  public static <T, U> CloseableIterator<U> wrap(final Iterator<T> iter,
          final Function<T, U> function) {
    return wrap(wrap(iter), function);
  }

  public static <T, U> CloseableIterator<U> wrap(final CloseableIterator<T> iter,
          final Function<T, U> function) {
    return new CloseableIterator<U>() {
      private U current;

      @Override
      public boolean hasNext() {
        while (true) {
          if (!iter.hasNext()) {
            return false;
          }
          current = function.apply(iter.next());
          if (current != null) {
            return true;
          }
        }
      }

      @Override
      public U next() {
        if (current == null && !hasNext()) {
          throw new NoSuchElementException();
        }
        try {
          return current;
        } finally {
          current = null;
        }
      }

      @Override
      public void close() throws IOException {
        iter.close();
      }
    };
  }

  public static <T> CloseableIterator<T> wrap(final Iterator<T> iter,
          final Comparator<? super T> comparator) {
    return wrap(wrap(iter), comparator);
  }

  public static <T> CloseableIterator<T> wrap(final CloseableIterator<T> iter,
          final Comparator<? super T> comparator) {
    if (ComparatorUtils.isEmptyComparator(comparator)) {
      return iter;
    }
    Set<T> sortedData = new TreeSet<>(comparator);
    while (iter.hasNext()) {
      sortedData.add(iter.next());
    }
    return new CloseableIterator<T>() {
      private final Iterator<T> sortedIter = sortedData.iterator();

      @Override
      public boolean hasNext() {
        return sortedIter.hasNext();
      }

      @Override
      public T next() {
        return sortedIter.next();
      }

      @Override
      public void close() throws IOException {
        iter.close();
      }
    };
  }

  public static <T> CloseableIterator<T> wrap(
          final Iterator<T> iter, final Predicate<T> predicate) {
    return wrap(wrap(iter), predicate);
  }

  public static <T> CloseableIterator<T> wrap(final List<CloseableIterator<T>> iters) {
    return new CloseableIterator<T>() {
      private int currentIndex = 0;
      private T currentObject = null;

      @Override
      public boolean hasNext() {
        while (true) {
          if (currentObject != null) {
            return true;
          }
          if (currentIndex >= iters.size()) {
            return false;
          }
          if (iters.get(currentIndex).hasNext()) {
            currentObject = iters.get(currentIndex).next();
            return true;
          }
          ++currentIndex;
        }
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        try {
          return Objects.requireNonNull(currentObject);
        } finally {
          currentObject = null;
        }
      }

      @Override
      public void close() throws IOException {
        iters.forEach(v -> TrekUtils.closeWithLog(v, null));
      }
    };
  }

  public static <T> CloseableIterator<T> wrap(final CloseableIterator<T> iter,
          final Predicate<T> predicate) {
    return new CloseableIterator<T>() {
      private T current = null;

      @Override
      public boolean hasNext() {
        while (true) {
          if (current != null) {
            return true;
          }
          if (!iter.hasNext()) {
            return false;
          }
          current = iter.next();
          if (predicate.test(current)) {
            return true;
          }
          current = null;
        }
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        try {
          return current;
        } finally {
          current = null;
        }
      }

      @Override
      public void close() throws IOException {
        iter.close();
      }
    };
  }

  public static <T> CloseableIterator<T> wrapLimit(
          final Iterator<T> iter, final int limit) {
    return wrapLimit(wrap(iter), limit);
  }

  public static <T> CloseableIterator<T> wrapLimit(
          final CloseableIterator<T> iter, final int limit) {
    return new CloseableIterator<T>() {
      private long count = 0;

      private boolean reachLimit() {
        return (count >= limit && limit >= 0);
      }

      @Override
      public boolean hasNext() {
        return !reachLimit() && iter.hasNext();
      }

      @Override
      public T next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        try {
          return iter.next();
        } finally {
          ++count;
        }
      }

      @Override
      public void close() throws IOException {
        iter.close();
      }
    };
  }

  public static <T> CloseableIterator<T> wrapOffset(
          final Iterator<T> iter, final int offset) {
    return wrapOffset(wrap(iter), offset);
  }

  public static <T> CloseableIterator<T> wrapOffset(
          final CloseableIterator<T> iter, final int offset) {
    for (int index = 0; index < offset && iter.hasNext(); ++index) {
      iter.next();
    }
    return new CloseableIterator<T>() {
      @Override
      public boolean hasNext() {
        return iter.hasNext();
      }

      @Override
      public T next() {
        return iter.next();
      }

      @Override
      public void close() throws IOException {
        iter.close();
      }
    };
  }

  private IteratorUtils() {
  }
}
