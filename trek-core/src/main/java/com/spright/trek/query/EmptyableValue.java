package com.spright.trek.query;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public abstract class EmptyableValue<T> {

  private final EmptyableState state;

  public EmptyableValue(final EmptyableState state) {
    this.state = state;
  }

  public T orElse(T other) {
    if (state == EmptyableState.HAS_VALUE) {
      return get();
    } else {
      return other;
    }
  }

  public EmptyableState getState() {
    return state;
  }

  protected abstract T get();

  public boolean isHasValue() {
    return state == EmptyableState.HAS_VALUE;
  }

  public EmptyableValue<T> ifNull(Runnable run) {
    if (state == EmptyableState.NULL) {
      run.run();
    }
    return this;
  }

  public EmptyableValue<T> ifEmpty(Runnable run) {
    if (state == EmptyableState.EMPTY) {
      run.run();
    }
    return this;
  }

  public EmptyableValue<T> ifHasValue(Consumer<? super T> consumer) {
    if (state == EmptyableState.HAS_VALUE) {
      consumer.accept(get());
    }
    return this;
  }

  public Optional<T> filter(Predicate<? super T> predicate) {
    Objects.requireNonNull(predicate);
    if (state == EmptyableState.HAS_VALUE) {
      return predicate.test(get()) ? Optional.of(get()) : Optional.empty();
    } else {
      return Optional.empty();
    }
  }

  public <U> Optional<U> map(Function<? super T, ? extends U> mapper) {
    Objects.requireNonNull(mapper);
    if (state == EmptyableState.HAS_VALUE) {
      return Optional.ofNullable(mapper.apply(get()));
    } else {
      return Optional.empty();
    }
  }
}
