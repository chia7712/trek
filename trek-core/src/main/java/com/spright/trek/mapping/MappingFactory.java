package com.spright.trek.mapping;

import com.spright.trek.exception.MappingIOException;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.DConstants;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import com.spright.trek.query.IteratorUtils;

public final class MappingFactory {

  public static Mapping newInstance(final Configuration conf) throws Exception {
    if (conf.getBoolean(DConstants.ENABLE_SINGLE_MODE,
            DConstants.DEFAULT_ENABLE_SINGLETON)) {
      return new InMemoryMapping();
    }
    return new MappingTable(conf, TableName.valueOf(
            conf.get(DConstants.TREK_NAMESPACE, DConstants.DEFAULT_TREK_NAMESPACE),
            DConstants.MAPPING_NAME));
  }

  private static class InMemoryMapping implements Mapping {

    private final ConcurrentMap<String, AccountInfo> accountMap = new ConcurrentSkipListMap<>();
    private volatile boolean closed = false;

    @Override
    public CloseableIterator<AccountInfo> list(AccountInfoQuery query) throws MappingIOException, IOException {
      return Optional.of(IteratorUtils.wrap(accountMap.values().iterator()))
              .map(iter -> IteratorUtils.wrap(iter, query.getPredicate()))
              .map(iter -> IteratorUtils.wrap(iter, query.getComparator()))
              .map(iter -> IteratorUtils.wrapOffset(iter, query.getOffset()))
              .map(iter -> IteratorUtils.wrapLimit(iter, query.getLimit()))
              .map(iter -> query.getKeep()
                      ? iter
                      : IteratorUtils.wrap(iter, (AccountInfo info) -> {
                        AccountInfo rval = null;
                        info.getId().ifPresent(id -> accountMap.remove(id));
                        return rval;
                      }))
              .get();
    }

    @Override
    public boolean supportOrder(AccountInfoQuery query) {
      return query.getOrderKey()
              .stream()
              .allMatch(v -> v.getKey() == AccountInfo.Field.ID);
    }

    @Override
    public boolean supportFilter(AccountInfoQuery query) {
      return false;
    }

    @Override
    public Optional<AccountInfo> find(String alias) throws MappingIOException {
      return Optional.ofNullable(accountMap.get(alias));
    }

    @Override
    public AccountInfo add(AccountInfoUpdate update) throws IOException {
      AccountInfo.Builder builder = AccountInfo.newBuilder();
      AccountInfo oriInfo = accountMap.getOrDefault(update.getId(), AccountInfo.EMPTY);
      builder.setId(update.getId());
      update.getDomain().ifHasValue(v -> builder.setDomain(v))
              .ifNull(() -> oriInfo.getDomain().ifPresent(v -> builder.setDomain(v)));
      update.getUser().ifHasValue(v -> builder.setUser(v))
              .ifNull(() -> oriInfo.getUser().ifPresent(v -> builder.setUser(v)));
      update.getPassword().ifHasValue(v -> builder.setPassword(v))
              .ifNull(() -> oriInfo.getPassword().ifPresent(v -> builder.setPassword(v)));
      update.getHost().ifHasValue(v -> builder.setHost(v))
              .ifNull(() -> oriInfo.getHost().ifPresent(v -> builder.setHost(v)));
      update.getPort().ifHasValue(v -> builder.setPort(v))
              .ifNull(() -> oriInfo.getPort().ifPresent(v -> builder.setPort(v)));
      AccountInfo info = builder.build();
      accountMap.put(update.getId(), info);
      return info;
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      closed = true;
      accountMap.clear();
    }

    @Override
    public CloseableIterator<AccountInfo> list() throws MappingIOException, IOException {
      return list(AccountInfoQuery.QUERY_ALL);
    }
  }

  private MappingFactory() {
  }
}
