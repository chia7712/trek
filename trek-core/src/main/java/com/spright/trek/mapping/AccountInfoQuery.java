package com.spright.trek.mapping;

import com.spright.trek.mapping.AccountInfo.Field;
import com.spright.trek.query.ComparatorUtils;
import com.spright.trek.query.PredicateUtils;
import java.util.Optional;
import com.spright.trek.query.ListQuery;
import com.spright.trek.query.OrderKey;
import com.spright.trek.query.QueryUtils;
import com.spright.trek.query.RangeInteger;
import com.spright.trek.utils.BaseBuilder;
import com.spright.trek.DConstants;
import java.util.ArrayList;
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

public class AccountInfoQuery implements ListQuery<Field, AccountInfo> {

  public static final Comparator<AccountInfo> DEFAULT_COMPARATOR = newComparator(null);
  public static final AccountInfoQuery QUERY_ALL = AccountInfoQuery.newBuilder().build();

  public static Builder newBuilder() {
    return new Builder();
  }

  public static AccountInfoQuery parse(final Map<String, String> rawQuery) {
    AccountInfoQuery.Builder builder = AccountInfoQuery.newBuilder();
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_MAPPING_ID))
            .ifPresent(v -> builder.setId(v));
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_MAPPING_DOMAIN))
            .ifPresent(v -> builder.setDomain(v));
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_MAPPING_USER))
            .ifPresent(v -> builder.setUser(v));
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_MAPPING_PASSWORD))
            .ifPresent(v -> builder.setPassword(v));
    QueryUtils.parseEmptyString(rawQuery.get(DConstants.URI_MAPPING_HOST))
            .ifPresent(v -> builder.setHost(v));
    QueryUtils.parsePositiveRangeInteger(rawQuery.get(DConstants.URI_MAPPING_PORT))
            .ifPresent(v -> builder.setPort(v));
    return builder.setLimit(QueryUtils.getLimit(rawQuery))
            .setOffset(QueryUtils.getOffset(rawQuery))
            .setOrderKeys(QueryUtils.getOrderKeys(rawQuery).stream()
                    .map(v -> Field.find(v.getKey()).map(k
                            -> new OrderKey<>(k, v.getAsc())).orElse(null))
                    .collect(Collectors.toCollection(() -> new LinkedHashSet<>())))
            .setKeep(QueryUtils.parseBoolean(DConstants.URI_DATA_KEEP, DConstants.DEFAULT_URI_DATA_KEEP))
            .build();
  }
  private final int offset;
  private final int limit;
  private final boolean keep;
  private final String id;
  private final String domain;
  private final String user;
  private final String password;
  private final String host;
  private final RangeInteger rangePort;
  private final Set<OrderKey<Field>> orderKeys;
  private final Comparator<AccountInfo> comparator;
  private final Predicate<AccountInfo> predicate;

  private AccountInfoQuery(final int offset, final int limit, boolean keep,
          final Set<OrderKey<Field>> orderKeys,
          final String id, final String domain,
          final String user, final String password,
          final String host, final RangeInteger rangePort) {
    this.offset = offset;
    this.limit = limit;
    this.keep = keep;
    this.id = id;
    this.domain = domain;
    this.user = user;
    this.password = password;
    this.host = host;
    this.rangePort = rangePort;
    this.orderKeys = orderKeys;
    this.comparator = newComparator(orderKeys);
    List<Predicate<AccountInfo>> predicates = new LinkedList<>();
    if (id != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getId().orElse(null),
              PredicateUtils.newPredicate(id)));
    }
    if (domain != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getDomain().orElse(null),
              PredicateUtils.newPredicate(domain)));
    }
    if (user != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getUser().orElse(null),
              PredicateUtils.newPredicate(user)));
    }
    if (password != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getPassword().orElse(null),
              PredicateUtils.newPredicate(password)));
    }
    if (host != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getHost().orElse(null),
              PredicateUtils.newPredicate(host)));
    }
    if (rangePort != null) {
      predicates.add(PredicateUtils.newPredicate(t -> t.getPort().orElse(null),
              PredicateUtils.newPredicate(rangePort)));
    }
    this.predicate = PredicateUtils.newPredicate(predicates);
  }

  public Optional<String> getId() {
    return Optional.ofNullable(id);
  }

  public Optional<String> getDomain() {
    return Optional.ofNullable(domain);
  }

  public Optional<String> getUser() {
    return Optional.ofNullable(user);
  }

  public Optional<String> getPassword() {
    return Optional.ofNullable(password);
  }

  public Optional<String> getHost() {
    return Optional.ofNullable(host);
  }

  public Optional<RangeInteger> getPortRange() {
    return Optional.ofNullable(rangePort);
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
  public Set<OrderKey<Field>> getOrderKey() {
    return new LinkedHashSet<>(orderKeys);
  }

  @Override
  public Predicate<AccountInfo> getPredicate() {
    return predicate;
  }

  @Override
  public Comparator<AccountInfo> getComparator() {
    return comparator;
  }

  private static Comparator<AccountInfo> newComparator(final Set<OrderKey<Field>> orderKeys) {
    Map<Field, OrderKey<Field>> allOrder = new LinkedHashMap<>();
    if (orderKeys != null) {
      orderKeys.forEach(v -> allOrder.putIfAbsent(v.getKey(), v));
    } else {
      Stream.of(Field.values()).forEach(v -> allOrder.putIfAbsent(v,
              new OrderKey<>(v, DConstants.DEFAULT_URI_DATA_ORDER_ASC)));
    }
    List<Comparator<AccountInfo>> comparators = new ArrayList<>(allOrder.size());
    allOrder.forEach((k, v) -> {
      switch (k) {
        case ID:
          comparators.add((v1, v2) -> v.getAsc()
                  ? compare(v1.getId(), v2.getId())
                  : compare(v2.getId(), v1.getId()));
          break;
        case DOMAIN:
          comparators.add((v1, v2) -> v.getAsc()
                  ? compare(v1.getDomain(), v2.getDomain())
                  : compare(v2.getDomain(), v1.getDomain()));
          break;
        case USER:
          comparators.add((v1, v2) -> v.getAsc()
                  ? compare(v1.getUser(), v2.getUser())
                  : compare(v2.getUser(), v1.getUser()));
          break;
        case PASSWORD:
          comparators.add((v1, v2) -> v.getAsc()
                  ? compare(v1.getHost(), v2.getHost())
                  : compare(v2.getHost(), v1.getHost()));
          break;
        case HOST:
          comparators.add((v1, v2) -> v.getAsc()
                  ? compare(v1.getPassword(), v2.getPassword())
                  : compare(v2.getPassword(), v1.getPassword()));
          break;
        case PORT:
          comparators.add((v1, v2) -> v.getAsc()
                  ? Integer.compare(v1.getPort().orElse(0), v2.getPort().orElse(0))
                  : Integer.compare(v2.getPort().orElse(0), v1.getPort().orElse(0)));
          break;
        default:
      }
    });
    return ComparatorUtils.newComparator(comparators);
  }

  private static int compare(final Optional<String> v1, final Optional<String> v2) {
    return v1.orElse(DConstants.EMPTY_STRING).compareTo(v2.orElse(DConstants.EMPTY_STRING));
  }

  @Override
  public boolean getKeep() {
    return keep;
  }

  public static class Builder extends BaseBuilder {

    private int offset = DConstants.DEFAULT_URI_DATA_OFFSET;
    private int limit = DConstants.DEFAULT_URI_DATA_LIMIT;
    private boolean keep = DConstants.DEFAULT_URI_DATA_KEEP;
    private String id;
    private String domain;
    private String user;
    private String password;
    private String host;
    private RangeInteger rangePort;
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

    public Builder setId(final String value) {
      if (isValid(value)) {
        id = value;
      }
      return this;
    }

    public Builder setDomain(final String value) {
      if (isValid(value)) {
        domain = value;
      }
      return this;
    }

    public Builder setUser(final String value) {
      if (isValid(value)) {
        user = value;
      }
      return this;
    }

    public Builder setPassword(final String value) {
      if (isValid(value)) {
        password = value;
      }
      return this;
    }

    public Builder setHost(final String value) {
      if (isValid(value)) {
        host = value;
      }
      return this;
    }

    public Builder setPort(final RangeInteger value) {
      if (isValid(value)) {
        rangePort = value;
      }
      return this;
    }

    public Builder setOrderKeys(final Set<OrderKey<Field>> value) {
      if (isValid(value)) {
        orderKeys = value;
      }
      return this;
    }

    public AccountInfoQuery build() {
      return new AccountInfoQuery(
              offset,
              limit,
              keep,
              orderKeys == null ? Collections.EMPTY_SET : new LinkedHashSet<>(orderKeys),
              id,
              domain,
              user,
              password,
              host,
              rangePort);
    }
  }
}
