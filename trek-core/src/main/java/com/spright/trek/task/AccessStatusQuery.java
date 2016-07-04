package com.spright.trek.task;

import com.spright.trek.query.ComparatorUtils;
import com.spright.trek.query.PredicateUtils;
import com.spright.trek.query.ListQuery;
import com.spright.trek.query.OrderKey;
import com.spright.trek.query.QueryUtils;
import com.spright.trek.query.RangeDouble;
import com.spright.trek.query.RangeLong;
import com.spright.trek.task.AccessStatus.Field;
import com.spright.trek.utils.BaseBuilder;
import com.spright.trek.DConstants;
import java.util.Optional;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AccessStatusQuery implements ListQuery<Field, AccessStatus> {

  public static final Comparator<AccessStatus> DEFAULT_COMPARATOR = createComparator(null);
  public static final AccessStatusQuery QUERY_ALL
          = AccessStatusQuery.newBuilder().build();

  public static AccessStatusQuery parse(final Map<String, String> rawQuery) {
    SimpleDateFormat timeSdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    AccessStatusQuery.Builder builder = AccessStatusQuery.newBuilder();
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_TASK_ID))
            .ifPresent(v -> builder.setId(v));
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_TASK_REDIRECT))
            .ifPresent(v -> builder.setRedirectFrom(v));
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_TASK_SERVER_NAME))
            .ifPresent(v -> builder.setServerName(v));
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_TASK_CLIENT_NAME))
            .ifPresent(v -> builder.setClientName(v));
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_TASK_FROM))
            .ifPresent(v -> builder.setFrom(v));
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_TASK_TO))
            .ifPresent(v -> builder.setTo(v));
    TaskState.find(rawQuery.get(DConstants.URI_TASK_STATE))
            .ifPresent(v -> builder.setTaskState(v));
    QueryUtils.parsePositiveRangeDouble(rawQuery.get(DConstants.URI_TASK_PROGRESS))
            .ifPresent(v -> builder.setProgress(v));
    QueryUtils.parsePositiveRangeTime(timeSdf, rawQuery.get(DConstants.URI_TASK_START_TIME))
            .ifPresent(v -> builder.setStartTime(v));
    QueryUtils.parsePositiveRangeLong(rawQuery.get(DConstants.URI_TASK_ELAPSED))
            .ifPresent(v -> builder.setElapsed(v));
    QueryUtils.parsePositiveRangeLong(rawQuery.get(DConstants.URI_TASK_EXPECTED_SIZE))
            .ifPresent(v -> builder.setExpectedSize(v));
    QueryUtils.parsePositiveRangeLong(rawQuery.get(DConstants.URI_TASK_TRANSFERRED_SIZE))
            .ifPresent(v -> builder.setTransferredSize(v));
    return builder.setLimit(QueryUtils.getLimit(rawQuery))
            .setOffset(QueryUtils.getOffset(rawQuery))
            .setOrderKeys(QueryUtils.getOrderKeys(rawQuery).stream()
                    .map(v -> Field.find(v.getKey()).map(k
                            -> new OrderKey<>(k, v.getAsc())).orElse(null))
                    .collect(Collectors.toCollection(() -> new LinkedHashSet<>())))
            .setKeep(QueryUtils.parseBoolean(rawQuery.get(DConstants.URI_DATA_KEEP), DConstants.DEFAULT_URI_DATA_KEEP))
            .build();
  }

  public static Builder newBuilder() {
    return new AccessStatusQuery.Builder();
  }
  private final int offset;
  private final int limit;
  private final boolean keep;
  private final String redirectFrom;
  private final String serverName;
  private final String clientName;
  private final String id;
  private final String from;
  private final String to;
  private final TaskState state;
  private final RangeDouble progress;
  private final RangeLong startTime;
  private final RangeLong elapsed;
  private final RangeLong expectedSize;
  private final RangeLong transferredSize;
  private final Set<OrderKey<Field>> orderKeys;
  private final Predicate<AccessStatus> predicate;
  private final Comparator<AccessStatus> comparator;

  public AccessStatusQuery(
          final int offset,
          final int limit,
          final boolean keep,
          final Set<OrderKey<Field>> orderKeys,
          final String redirectFrom,
          final String serverName,
          final String clientName,
          final String id,
          final String from,
          final String to,
          final TaskState state,
          final RangeDouble progress,
          final RangeLong startTime,
          final RangeLong elapsed,
          final RangeLong expectedSize,
          final RangeLong transferredSize) {
    this.offset = offset;
    this.limit = limit;
    this.keep = keep;
    this.redirectFrom = redirectFrom;
    this.serverName = serverName;
    this.clientName = clientName;
    this.id = id;
    this.from = from;
    this.to = to;
    this.state = state;
    this.progress = progress;
    this.startTime = startTime;
    this.elapsed = elapsed;
    this.expectedSize = expectedSize;
    this.transferredSize = transferredSize;
    this.orderKeys = orderKeys;
    this.comparator = createComparator(orderKeys);
    List<Predicate<AccessStatus>> predicates = new LinkedList<>();

    if (redirectFrom != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getRedirectFrom(),
              PredicateUtils.newPredicate(redirectFrom)));
    }
    if (id != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getId(),
              PredicateUtils.newPredicate(id)));
    }
    if (serverName != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getServerName(),
              PredicateUtils.newPredicate(serverName)));
    }
    if (clientName != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getClientName(),
              PredicateUtils.newPredicate(clientName)));
    }
    if (from != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getFrom(),
              PredicateUtils.newPredicate(from)));
    }
    if (to != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getTo(),
              PredicateUtils.newPredicate(to)));
    }
    if (state != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getState(),
              PredicateUtils.newPredicate(state)));
    }
    if (progress != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getProgress(),
              PredicateUtils.newPredicate(progress)));
    }
    if (startTime != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getStartTime(),
              PredicateUtils.newPredicate(startTime)));
    }
    if (elapsed != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getElapsed(),
              PredicateUtils.newPredicate(elapsed)));
    }
    if (expectedSize != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getExpectedSize(),
              PredicateUtils.newPredicate(expectedSize)));
    }
    if (transferredSize != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getTransferredSize(),
              PredicateUtils.newPredicate(transferredSize)));
    }
    this.predicate = PredicateUtils.newPredicate(predicates);
  }

  public Optional<String> getRedirectFrom() {
    return Optional.ofNullable(redirectFrom);
  }

  public Optional<String> getServerName() {
    return Optional.ofNullable(serverName);
  }

  public Optional<String> getClientName() {
    return Optional.ofNullable(clientName);
  }

  public Optional<String> getId() {
    return Optional.ofNullable(id);
  }

  public Optional<String> getFrom() {
    return Optional.ofNullable(from);
  }

  public Optional<String> getTo() {
    return Optional.ofNullable(to);
  }

  public Optional<TaskState> getState() {
    return Optional.ofNullable(state);
  }

  public Optional<RangeDouble> getProgress() {
    return Optional.ofNullable(progress);
  }

  public Optional<RangeLong> getStartTime() {
    return Optional.ofNullable(startTime);
  }

  public Optional<RangeLong> getElapsed() {
    return Optional.ofNullable(elapsed);
  }

  public Optional<RangeLong> getExpectedSize() {
    return Optional.ofNullable(expectedSize);
  }

  public Optional<RangeLong> getTransferredSize() {
    return Optional.ofNullable(transferredSize);
  }

  @Override
  public Set<OrderKey<Field>> getOrderKey() {
    return new LinkedHashSet<>(orderKeys);
  }

  @Override
  public int getLimit() {
    return limit;
  }

  @Override
  public int getOffset() {
    return offset;
  }

  @Override
  public Predicate<AccessStatus> getPredicate() {
    return predicate;
  }

  @Override
  public Comparator<AccessStatus> getComparator() {
    return comparator;
  }

  @Override
  public boolean getKeep() {
    return keep;
  }

  public static class Builder extends BaseBuilder {

    private int offset = DConstants.DEFAULT_URI_DATA_OFFSET;
    private int limit = DConstants.DEFAULT_URI_DATA_LIMIT;
    private boolean keep = DConstants.DEFAULT_URI_DATA_KEEP;
    private String redirectFrom;
    private String serverName;
    private String clientName;
    private String id;
    private String from;
    private String to;
    private TaskState state;
    private RangeDouble progress;
    private RangeLong startTime;
    private RangeLong elapsed;
    private RangeLong expectedSize;
    private RangeLong transferredSize;
    private Set<OrderKey<Field>> orderKeys;

    public Builder setKeep(final boolean value) {
      if (isValid(value)) {
        keep = value;
      }
      return this;
    }

    public Builder setOffset(final int value) {
      if (isValid(value)) {
        offset = value;
      }
      return this;
    }

    public Builder setLimit(final int value) {
      if (isValid(value)) {
        limit = value;
      }
      return this;
    }

    public Builder setRedirectFrom(final String value) {
      if (isValid(value)) {
        redirectFrom = value;
      }
      return this;
    }

    public Builder setServerName(final String value) {
      if (isValid(value)) {
        serverName = value;
      }
      return this;
    }

    public Builder setClientName(final String value) {
      if (isValid(value)) {
        clientName = value;
      }
      return this;
    }

    public Builder setId(final String value) {
      if (isValid(value)) {
        id = value;
      }
      return this;
    }

    public Builder setFrom(final String value) {
      if (isValid(value)) {
        from = value;
      }
      return this;
    }

    public Builder setTo(final String value) {
      if (isValid(value)) {
        to = value;
      }
      return this;
    }

    public Builder setTaskState(final TaskState value) {
      if (isValid(value)) {
        state = value;
      }
      return this;
    }

    public Builder setProgress(final RangeDouble value) {
      if (isValid(value)) {
        progress = value;
      }
      return this;
    }

    public Builder setTransferredSize(final RangeLong value) {
      if (isValid(value)) {
        transferredSize = value;
      }
      return this;
    }

    public Builder setStartTime(final RangeLong value) {
      if (isValid(value)) {
        startTime = value;
      }
      return this;
    }

    public Builder setElapsed(final RangeLong value) {
      if (isValid(value)) {
        elapsed = value;
      }
      return this;
    }

    public Builder setExpectedSize(final RangeLong value) {
      if (isValid(value)) {
        expectedSize = value;
      }
      return this;
    }

    public Builder setOrderKeys(final Set<OrderKey<Field>> value) {
      if (isValid(value)) {
        orderKeys = value;
      }
      return this;

    }

    public AccessStatusQuery build() {
      return new AccessStatusQuery(
              offset,
              limit,
              keep,
              orderKeys == null ? Collections.EMPTY_SET : new LinkedHashSet<>(orderKeys),
              redirectFrom,
              serverName,
              clientName,
              id,
              from,
              to,
              state,
              progress,
              startTime,
              elapsed,
              expectedSize,
              transferredSize);
    }
  }

  private static Comparator<AccessStatus> createComparator(final Set<OrderKey<Field>> orderKeys) {
    Map<Field, OrderKey<Field>> allOrder = new LinkedHashMap<>();
    if (orderKeys != null) {
      orderKeys.forEach(v -> allOrder.putIfAbsent(v.getKey(), v));
    } else {
      Stream.of(Field.values()).forEach(v -> allOrder.putIfAbsent(v,
              new OrderKey<>(v, DConstants.DEFAULT_URI_DATA_ORDER_ASC)));
    }

    List<Comparator<AccessStatus>> comparators = new LinkedList<>();
    allOrder.forEach((k, v) -> {
      switch (k) {
        case ID:
          comparators.add((v1, v2) -> v.getAsc()
                  ? v1.getId().compareTo(v2.getId())
                  : v2.getId().compareTo(v1.getId()));
          break;
        case REDIRECT_FROM:
          comparators.add((v1, v2) -> v.getAsc()
                  ? v1.getRedirectFrom().compareTo(v2.getRedirectFrom())
                  : v2.getRedirectFrom().compareTo(v1.getRedirectFrom()));
          break;
        case SERVER_NAME:
          comparators.add((v1, v2) -> v.getAsc()
                  ? v1.getServerName().compareTo(v2.getServerName())
                  : v2.getServerName().compareTo(v1.getServerName()));
          break;
        case CLIENT_NAME:
          comparators.add((v1, v2) -> v.getAsc()
                  ? v1.getClientName().compareTo(v2.getClientName())
                  : v2.getClientName().compareTo(v1.getClientName()));
          break;
        case FROM:
          comparators.add((v1, v2) -> v.getAsc()
                  ? v1.getFrom().compareTo(v2.getFrom())
                  : v2.getFrom().compareTo(v1.getFrom()));
          break;
        case TO:
          comparators.add((v1, v2) -> v.getAsc()
                  ? v1.getTo().compareTo(v2.getTo())
                  : v2.getTo().compareTo(v1.getTo()));
          break;
        case STATE:
          comparators.add((v1, v2) -> v.getAsc()
                  ? v1.getState().compareTo(v2.getState())
                  : v2.getState().compareTo(v1.getState()));
          break;
        case PROGRESS:
          comparators.add((v1, v2) -> v.getAsc()
                  ? Double.compare(v1.getProgress(), v2.getProgress())
                  : Double.compare(v2.getProgress(), v1.getProgress()));
          break;
        case START_TIME:
          comparators.add((v1, v2) -> v.getAsc()
                  ? Long.compare(v1.getStartTime(), v2.getStartTime())
                  : Long.compare(v2.getStartTime(), v1.getStartTime()));
          break;
        case ELAPSED:
          comparators.add((v1, v2) -> v.getAsc()
                  ? Long.compare(v1.getElapsed(), v2.getElapsed())
                  : Long.compare(v2.getElapsed(), v1.getElapsed()));
          break;
        case EXPECTED_SIZE:
          comparators.add((v1, v2) -> v.getAsc()
                  ? Long.compare(v1.getExpectedSize(), v2.getExpectedSize())
                  : Long.compare(v2.getExpectedSize(), v1.getExpectedSize()));
          break;
        case TRANSFERRED_SIZE:
          comparators.add((v1, v2) -> v.getAsc()
                  ? Long.compare(v1.getTransferredSize(), v2.getTransferredSize())
                  : Long.compare(v2.getTransferredSize(), v1.getTransferredSize()));
          break;
        default:
          throw new RuntimeException("No suitable comparator for " + k);
      }
    });
    return ComparatorUtils.newComparator(comparators);
  }
}
