package com.spright.trek.datasystem.request;

import com.spright.trek.datasystem.request.DataInfo.Field;
import com.spright.trek.exception.MappingIOException;
import com.spright.trek.exception.UriParseIOException;
import com.spright.trek.mapping.Mapping;
import com.spright.trek.query.ComparatorUtils;
import java.util.List;
import java.util.Optional;
import com.spright.trek.query.ListQuery;
import com.spright.trek.query.OrderKey;
import com.spright.trek.query.PredicateUtils;
import com.spright.trek.query.QueryUtils;
import com.spright.trek.query.RangeLong;
import com.spright.trek.utils.BaseBuilder;
import com.spright.trek.DConstants;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class DataInfoQuery extends UriRequest implements ListQuery<Field, DataInfo> {

  public static Builder newBuilder() {
    return new Builder();
  }

  private static List<DataType> parseDataTypes(final Map<String, String> rawQuery) {
    List<DataType> types = new LinkedList<>();
    if (QueryUtils.parseBoolean(rawQuery.get(DConstants.URI_DATA_NEED_FILE),
            DConstants.DEFAULT_URI_DATA_NEED_FILE)) {
      types.add(DataType.FILE);
    }
    if (QueryUtils.parseBoolean(rawQuery.get(DConstants.URI_DATA_NEED_DIRECTORY),
            DConstants.DEFAULT_URI_DATA_NEED_DIRECTORY)) {
      types.add(DataType.DIRECTORY);
    }
    return types;
  }

  public static DataInfoQuery parse(final Map<String, String> rawQuery, final Mapping mapping) throws UriParseIOException, MappingIOException {
    DataInfoQuery.Builder builder = DataInfoQuery.newBuilder();
    SimpleDateFormat timeSdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    QueryUtils.parsePositiveRangeTime(timeSdf, rawQuery.get(DConstants.URI_DATA_UPLOAD_TIME))
            .ifPresent(v -> builder.setUploadTime(v));
    QueryUtils.parsePositiveRangeLong(rawQuery.get(DConstants.URI_DATA_SIZE))
            .ifPresent(v -> builder.setSize(v));
    return builder.setLimit(QueryUtils.getLimit(rawQuery))
            .setOffset(QueryUtils.getOffset(rawQuery))
            .setOrderKeys(QueryUtils.getOrderKeys(rawQuery).stream()
                    .map(v -> Field.find(v.getKey()).map(k
                            -> new OrderKey<>(k, v.getAsc())).orElse(null))
                    .collect(Collectors.toCollection(() -> new LinkedHashSet<>())))
            .setUriRequest(UriRequest.parseFrom(rawQuery, mapping, null))
            .setTypes(parseDataTypes(rawQuery))
            .setKeep(QueryUtils.parseBoolean(rawQuery.get(DConstants.URI_DATA_KEEP),
                    DConstants.DEFAULT_URI_DATA_KEEP))
            .build();
  }
  private final int offset;
  private final int limit;
  private final Set<OrderKey<Field>> orderKeys;
  private final List<DataType> types;
  private final RangeLong uploadTime;
  private final RangeLong size;
  private final boolean keep;
  private final Predicate<DataInfo> predicate;
  private final Comparator<DataInfo> comparator;

  public DataInfoQuery(final int offset, final int limit,
          final Set<OrderKey<Field>> orderKeys, final UriRequest request,
          final RangeLong uploadTime, final RangeLong size,
          final List<DataType> types, final boolean keep) {
    super(request);
    this.offset = offset;
    this.limit = limit;
    this.orderKeys = orderKeys;
    this.uploadTime = uploadTime;
    this.size = size;
    this.types = types;
    this.keep = keep;
    this.comparator = createComparator(orderKeys);
    List<Predicate<DataInfo>> predicates = new LinkedList<>();
    predicates.add(PredicateUtils.newPredicate(v -> v.getUriRequest().getPath().getName(),
            PredicateUtils.newPredicate(request.getPath().getName())));
    if (!types.isEmpty()) {
      predicates.add(PredicateUtils.newPredicate(v -> v.getType(),
              v -> types.stream().anyMatch(type -> type == v)));
    }
    if (uploadTime != null) {
      predicates.add(PredicateUtils.newPredicate(v -> v.getUploadTime(),
              PredicateUtils.newPredicate(uploadTime)));
    }
    if (size != null) {
      predicates.add(PredicateUtils.newPredicate(v -> v.getSize(),
              PredicateUtils.newPredicate(size)));
    }
    this.predicate = PredicateUtils.newPredicate(predicates);
  }

  @Override
  public boolean getKeep() {
    return keep;
  }

  public List<DataType> getTypes() {
    return new ArrayList<>(types);
  }

  public Optional<RangeLong> getTime() {
    return Optional.ofNullable(uploadTime);
  }

  public Optional<RangeLong> getSize() {
    return Optional.ofNullable(size);
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
  public Predicate<DataInfo> getPredicate() {
    return predicate;
  }

  @Override
  public Comparator<DataInfo> getComparator() {
    return comparator;
  }

  private static Comparator<DataInfo> createComparator(final Set<OrderKey<Field>> orderKeys) {
    Map<Field, OrderKey<Field>> allOrder = new LinkedHashMap<>();
    if (orderKeys != null) {
      orderKeys.forEach(v -> allOrder.putIfAbsent(v.getKey(), v));
    } else {
      Stream.of(Field.values()).forEach(v -> allOrder.putIfAbsent(v,
              new OrderKey<>(v, DConstants.DEFAULT_URI_DATA_ORDER_ASC)));
    }
    List<Comparator<DataInfo>> comparators = new LinkedList<>();
    allOrder.forEach((k, v) -> {
      switch (k) {
        case UPLOAD_TIME:
          comparators.add((v1, v2) -> v.getAsc()
                  ? Long.compare(v1.getUploadTime(), v2.getUploadTime())
                  : Long.compare(v2.getUploadTime(), v1.getUploadTime()));
          break;
        case SIZE:
          comparators.add((v1, v2) -> v.getAsc()
                  ? Long.compare(v1.getSize(), v2.getSize())
                  : Long.compare(v2.getSize(), v1.getSize()));
          break;
        case TYPE:
          comparators.add((v1, v2) -> v.getAsc()
                  ? v1.getType().compareTo(v2.getType())
                  : v2.getType().compareTo(v1.getType()));
          break;
        case NAME:
          comparators.add((v1, v2) -> v.getAsc()
                  ? v1.getUriRequest().getPath().getName().compareTo(v2.getUriRequest().getPath().getName())
                  : v2.getUriRequest().getPath().getName().compareTo(v1.getUriRequest().getPath().getName()));
          break;
        default:
          throw new RuntimeException("No suitable comparator for " + k);
      }
    });
    return ComparatorUtils.newComparator(comparators);
  }

  public static class Builder extends BaseBuilder {

    private int offset = DConstants.DEFAULT_URI_DATA_OFFSET;
    private int limit = DConstants.DEFAULT_URI_DATA_LIMIT;
    private boolean keep = DConstants.DEFAULT_URI_DATA_KEEP;
    private Set<OrderKey<Field>> orderKeys;
    private UriRequest request;
    private List<DataType> types;
    private RangeLong uploadTime;
    private RangeLong size;

    public Builder setKeep(final boolean v) {
      keep = v;
      return this;
    }

    public Builder setOffset(final int v) {
      if (isValid(v)) {
        offset = v;
      }
      return this;
    }

    public Builder setLimit(final int v) {
      if (isValid(v)) {
        limit = v;
      }
      return this;
    }

    public Builder setOrderKeys(final Set<OrderKey<Field>> v) {
      if (isValid(v)) {
        orderKeys = v;
      }
      return this;
    }

    public Builder setUriRequest(final UriRequest v) {
      if (isValid(v)) {
        request = v;
      }
      return this;
    }

    public Builder setTypes(final List<DataType> v) {
      if (isValid(v)) {
        types = v;
      }
      return this;
    }

    public Builder setUploadTime(final RangeLong v) {
      if (isValid(v)) {
        uploadTime = v;
      }
      return this;
    }

    public Builder setSize(final RangeLong v) {
      if (isValid(v)) {
        size = v;
      }
      return this;
    }

    public DataInfoQuery build() {
      checkNull(request, "request");
      return new DataInfoQuery(
              offset,
              limit,
              orderKeys == null ? Collections.EMPTY_SET : new LinkedHashSet<>(orderKeys),
              request,
              uploadTime,
              size,
              types == null ? Arrays.asList(DataType.FILE) : new ArrayList<>(types),
              keep
      );
    }
  }
}
