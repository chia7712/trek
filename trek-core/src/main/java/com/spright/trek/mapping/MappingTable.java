package com.spright.trek.mapping;

import com.spright.trek.exception.MappingIOException;
import com.spright.trek.io.AtomicCloseable;
import com.spright.trek.mapping.AccountInfo.Field;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.query.IteratorUtils;
import com.spright.trek.query.OrderKey;
import com.spright.trek.thread.ShareableObject;
import com.spright.trek.DConstants;
import com.spright.trek.query.TableIterator;
import com.spright.trek.utils.TrekUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class MappingTable extends AtomicCloseable implements Mapping {

  private static final Log LOG = LogFactory.getLog(MappingTable.class);
  private final ShareableObject<Connection> conn;
  private final TableName tableName;
  private final int batchSize;

  public MappingTable(final Configuration conf, final TableName tableName) throws Exception {
    this.conn = ShareableObject.create(Connection.class,
            () -> ConnectionFactory.createConnection(conf));
    this.tableName = tableName;
    this.batchSize = conf.getInt(DConstants.OPERATION_BATCH_LIMIT,
            DConstants.DEFAULT_OPERATION_BATCH_LIMIT);
    try (Admin admin = conn.get().getAdmin()) {
      TrekUtils.chekcHBaseNamespace(admin, conf.get(
              DConstants.TREK_NAMESPACE,
              DConstants.DEFAULT_TREK_NAMESPACE));
      HTableDescriptor tableDesc = new HTableDescriptor(tableName);
      HColumnDescriptor columnDesc = new HColumnDescriptor(DConstants.MAPPING_TABLE_FAMILY);
      columnDesc.setMaxVersions(1);
      columnDesc.setInMemory(true);
      columnDesc.setCacheDataOnWrite(true);
      columnDesc.setCacheIndexesOnWrite(true);
      tableDesc.addFamily(columnDesc);
      if (admin.tableExists(tableName)) {
        return;
      }
      admin.createTable(tableDesc);
    } catch (IOException e) {
      conn.close();
      throw e;
    }
  }

  @Override
  public boolean supportOrder(final AccountInfoQuery query) {
    return query.getOrderKey()
            .stream()
            .allMatch(v -> v.getKey() == AccountInfo.Field.ID);
  }

  @Override
  public CloseableIterator<AccountInfo> list(final AccountInfoQuery query)
          throws MappingIOException, IOException {
    if (query.getLimit() == 0) {
      return IteratorUtils.empty();
    }
    FilterList filters = new FilterList();
    query.getDomain().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.MAPPING_TABLE_FAMILY,
              DConstants.MAPPING_DOMAIN_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(TrekUtils.wildcardToRegular(v))));
    });
    query.getUser().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.MAPPING_TABLE_FAMILY,
              DConstants.MAPPING_USER_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(TrekUtils.wildcardToRegular(v))));
    });
    query.getPassword().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.MAPPING_TABLE_FAMILY,
              DConstants.MAPPING_PASSWORD_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(TrekUtils.wildcardToRegular(v))));
    });
    query.getHost().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.MAPPING_TABLE_FAMILY,
              DConstants.MAPPING_HOST_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(TrekUtils.wildcardToRegular(v))));
    });
    query.getPortRange().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.MAPPING_TABLE_FAMILY,
              DConstants.MAPPING_PORT_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.MAPPING_TABLE_FAMILY,
              DConstants.MAPPING_PORT_QUALIFIER,
              CompareFilter.CompareOp.LESS,
              Bytes.toBytes(v.getMaxValue())));
    });
    filters.getFilters()
            .stream()
            .map(v -> v instanceof SingleColumnValueFilter
                    ? (SingleColumnValueFilter) v
                    : null)
            .forEach(v -> v.setFilterIfMissing(true));
    Get get = query.getId()
            .filter(v -> !TrekUtils.hasWildcardToRegular(v))
            .map(v -> new Get(Bytes.toBytes(v)))
            .orElse(null);
    if (get != null) {
      if (!filters.getFilters().isEmpty()) {
        get.setFilter(filters);
      }
      try (Table table = conn.get().getTable(tableName)) {
        AccountInfo info = toAccountInfo(table.get(get));
        return IteratorUtils.wrap(info);
      }
    } else {
      query.getId().ifPresent(v -> {
        filters.addFilter(new RowFilter(
                CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(TrekUtils.wildcardToRegular(v))));
      });
      Scan scan = new Scan();
      Set<OrderKey<Field>> orderKeys = query.getOrderKey();
      if (orderKeys.stream().allMatch(v -> v.getKey() == Field.ID)) {
        scan.setReversed(!orderKeys.stream().findFirst().get().getAsc());
      }
      scan.setCaching(DConstants.SCANNER_CACHING);
      if (!filters.getFilters().isEmpty()) {
        scan.setFilter(filters);
      }
      TableIterator tableIter = new TableIterator(conn.get().getTable(tableName), scan);
      return Optional.of(IteratorUtils.wrap(tableIter,
              (Result result) -> toAccountInfo(result)))
              .map(iter -> IteratorUtils.wrap(iter, query.getComparator()))
              .map(iter -> IteratorUtils.wrapOffset(iter, query.getOffset()))
              .map(iter -> IteratorUtils.wrapLimit(iter, query.getLimit()))
              .map(iter -> query.getKeep()
                      ? iter
                      : IteratorUtils.wrap(iter, (List<AccountInfo> infos) -> {
                        if (infos.isEmpty()) {
                          return new ArrayList<>(0);
                        }
                        Map<String, AccountInfo> mapInfos = new HashMap<>();
                        infos.forEach((AccountInfo info)
                                -> info.getId().map(id -> mapInfos.put(id, info)));
                        List<Delete> deletes = mapInfos.keySet()
                                .stream()
                                .map(v -> new Delete(Bytes.toBytes(v)))
                                .collect(Collectors.toList());
                        try {
                          tableIter.getTable().delete(deletes);
                          return new ArrayList<>(0);
                        } catch (IOException ex) {
                          return deletes.stream()
                                  .map(d -> mapInfos.get(Bytes.toString(d.getRow())))
                                  .collect(Collectors.toList());
                        }
                      }, batchSize))
              .get();
    }
  }

  @Override
  public Optional<AccountInfo> find(final String alias) throws MappingIOException {
    try (Table table = conn.get().getTable(tableName)) {
      Get get = new Get(Bytes.toBytes(alias));
      get.addFamily(DConstants.MAPPING_TABLE_FAMILY);
      Result r = table.get(get);
      if (r.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(toAccountInfo(r));
    } catch (IOException e) {
      throw new MappingIOException("Failed to open mapping table", e);
    }
  }

  @Override
  public AccountInfo add(final AccountInfoUpdate update) throws MappingIOException {
    try (Table table = conn.get().getTable(tableName)) {
      Put put = new Put(Bytes.toBytes(update.getId()));
      Delete delete = new Delete(put.getRow());
      update.getDomain().ifHasValue(v -> {
        put.addColumn(DConstants.MAPPING_TABLE_FAMILY,
                DConstants.MAPPING_DOMAIN_QUALIFIER,
                Bytes.toBytes(v));
      }).ifEmpty(() -> {
        delete.addColumns(DConstants.MAPPING_TABLE_FAMILY,
                DConstants.MAPPING_DOMAIN_QUALIFIER);
      });
      update.getUser().ifHasValue(v -> {
        put.addColumn(DConstants.MAPPING_TABLE_FAMILY,
                DConstants.MAPPING_USER_QUALIFIER,
                Bytes.toBytes(v));
      }).ifEmpty(() -> {
        delete.addColumns(DConstants.MAPPING_TABLE_FAMILY,
                DConstants.MAPPING_USER_QUALIFIER);
      });
      update.getPassword().ifHasValue(v -> {
        put.addColumn(DConstants.MAPPING_TABLE_FAMILY,
                DConstants.MAPPING_PASSWORD_QUALIFIER,
                Bytes.toBytes(v));
      }).ifEmpty(() -> {
        delete.addColumns(DConstants.MAPPING_TABLE_FAMILY,
                DConstants.MAPPING_PASSWORD_QUALIFIER);
      });
      update.getHost().ifHasValue(v -> {
        put.addColumn(DConstants.MAPPING_TABLE_FAMILY,
                DConstants.MAPPING_HOST_QUALIFIER,
                Bytes.toBytes(v));
      }).ifEmpty(() -> {
        delete.addColumns(DConstants.MAPPING_TABLE_FAMILY,
                DConstants.MAPPING_HOST_QUALIFIER);
      });
      update.getPort().ifHasValue(v -> put.addColumn(
              DConstants.MAPPING_TABLE_FAMILY,
              DConstants.MAPPING_PORT_QUALIFIER,
              Bytes.toBytes(v)))
              .ifEmpty(() -> delete.addColumn(DConstants.MAPPING_TABLE_FAMILY,
                      DConstants.MAPPING_PORT_QUALIFIER));
      if (!put.isEmpty()) {
        table.put(put);
      }
      if (!delete.isEmpty()) {
        table.delete(delete);
      }
      Get get = new Get(Bytes.toBytes(update.getId()));
      return toAccountInfo(table.get(get));
    } catch (IOException e) {
      throw new MappingIOException("Failed to open mapping table", e);
    }
  }

  @Override
  protected void internalClose() throws Exception {
    conn.close();
  }

  private static String toString(Result result, final byte[] qualifier) {
    byte[] data = result.getValue(DConstants.MAPPING_TABLE_FAMILY, qualifier);
    if (data == null || data.length == 0) {
      return null;
    }
    return Bytes.toString(data);
  }

  private static int toInteger(Result result, final byte[] qualifier) {
    byte[] data = result.getValue(DConstants.MAPPING_TABLE_FAMILY, qualifier);
    if (data == null || data.length == 0 || data.length != Bytes.SIZEOF_INT) {
      return -1;
    }
    return Bytes.toInt(data);
  }

  private static AccountInfo toAccountInfo(Result r) {
    return AccountInfo.newBuilder()
            .setId(Bytes.toString(r.getRow()))
            .setDomain(toString(r, DConstants.MAPPING_DOMAIN_QUALIFIER))
            .setUser(toString(r, DConstants.MAPPING_USER_QUALIFIER))
            .setPassword(toString(r, DConstants.MAPPING_PASSWORD_QUALIFIER))
            .setHost(toString(r, DConstants.MAPPING_HOST_QUALIFIER))
            .setPort(toInteger(r, DConstants.MAPPING_DOMAIN_QUALIFIER))
            .build();
  }

  @Override
  public boolean supportFilter(AccountInfoQuery query) {
    return true;
  }

  @Override
  public CloseableIterator<AccountInfo> list() throws MappingIOException, IOException {
    return list(AccountInfoQuery.QUERY_ALL);
  }
}
