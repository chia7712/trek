package com.spright.trek.task;

import com.spright.trek.exception.LogIOException;
import com.spright.trek.exception.TaskIOException;
import com.spright.trek.io.AtomicCloseable;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.query.IteratorUtils;
import com.spright.trek.query.OrderKey;
import com.spright.trek.query.TableIterator;
import com.spright.trek.task.AccessStatus.Field;
import com.spright.trek.thread.ShareableObject;
import com.spright.trek.DConstants;
import com.spright.trek.utils.HBaseUtils;
import com.spright.trek.utils.TrekUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
import org.apache.hadoop.hbase.client.Durability;
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

public class AccessTaskLogTable extends AtomicCloseable implements AccessTaskLogger {

  private static final Log LOG = LogFactory.getLog(AccessTaskLogTable.class);
  private static final int BUFFERED_SIZE = 100;
  private final ShareableObject<Connection> conn;
  private final TableName tableName;
  private final BlockingQueue<AccessStatus> buffer = new ArrayBlockingQueue<>(BUFFERED_SIZE);
  private final ExecutorService service = Executors.newFixedThreadPool(2);
  private final int batchSize;

  public AccessTaskLogTable(final Configuration conf, final TableName tableName) throws Exception {
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
      tableDesc.setDurability(Durability.SKIP_WAL);
      HColumnDescriptor columnDesc = new HColumnDescriptor(DConstants.TASK_FAMILY);
      columnDesc.setMaxVersions(1);
      int ttl = conf.getInt(
              DConstants.ACCESS_LOG_TTL_IN_SECOND,
              DConstants.DEFAULT_ACCESS_LOG_TTL_IN_SECOND);
      if (ttl > 0) {
        columnDesc.setTimeToLive(ttl);
      }
      tableDesc.addFamily(columnDesc);
      if (!admin.tableExists(tableName)) {
        admin.createTable(tableDesc);
      }
      Table table = conn.get().getTable(tableName);
      service.execute(() -> {
        try {
          List<Put> undo = null;
          while (!Thread.interrupted()) {
            try {
              undo = copyData(undo);
              table.put(undo);
              undo = null;
            } catch (IOException e) {
              LOG.error(e);
            }
          }
        } catch (InterruptedException e) {
          LOG.error("Failed to log the access status", e);
        } finally {
          TrekUtils.closeWithLog(table, LOG);
        }
      });
    } catch (IOException e) {
      conn.close();
      throw e;
    }

  }

  private List<Put> copyData(final List<Put> undo) throws InterruptedException {
    AccessStatus status = buffer.take();
    Map<String, Put> data = new TreeMap<>();
    if (undo != null) {
      undo.forEach(v -> data.put(Bytes.toString(v.getRow()), v));
    }
    //remove the duplicate data by comparing the id
    do {
      data.put(status.getId(), toPut(status));
    } while ((status = buffer.poll()) != null);
    return data.values()
            .stream()
            .collect(Collectors.toList());
  }

  @Override
  public boolean supportOrder(final AccessStatusQuery query) {
    return query.getOrderKey()
            .stream()
            .allMatch(v -> v.getKey() == AccessStatus.Field.ID);
  }

  @Override
  protected void internalClose() throws Exception {
    TrekUtils.closeWithLog(() -> conn.close(), LOG);
    service.shutdownNow();
    service.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
  }

  private static Put toPut(AccessStatus status) {
    Put put = new Put(Bytes.toBytes(status.getId()));
    put.addColumn(DConstants.TASK_FAMILY,
            DConstants.TASK_REDIRECT_QUALIFIER,
            Bytes.toBytes(status.getRedirectFrom()))
            .addColumn(DConstants.TASK_FAMILY,
                    DConstants.TASK_SERVER_QUALIFIER,
                    Bytes.toBytes(status.getServerName()))
            .addColumn(DConstants.TASK_FAMILY,
                    DConstants.TASK_CLIENT_QUALIFIER,
                    Bytes.toBytes(status.getClientName()))
            .addColumn(DConstants.TASK_FAMILY,
                    DConstants.TASK_FROM_QUALIFIER,
                    Bytes.toBytes(status.getFrom()))
            .addColumn(DConstants.TASK_FAMILY,
                    DConstants.TASK_TO_QUALIFIER,
                    Bytes.toBytes(status.getTo()))
            .addColumn(DConstants.TASK_FAMILY,
                    DConstants.TASK_STATE_QUALIFIER,
                    status.getState().getCode())
            .addColumn(DConstants.TASK_FAMILY,
                    DConstants.TASK_PROGRESS_QUALIFIER,
                    Bytes.toBytes(status.getProgress()))
            .addColumn(DConstants.TASK_FAMILY,
                    DConstants.TASK_START_TIME_QUALIFIER,
                    Bytes.toBytes(status.getStartTime()))
            .addColumn(DConstants.TASK_FAMILY,
                    DConstants.TASK_ELAPSED_QUALIFIER,
                    Bytes.toBytes(status.getElapsed()))
            .addColumn(DConstants.TASK_FAMILY,
                    DConstants.TASK_EXPECTED_SIZE_QUALIFIER,
                    Bytes.toBytes(status.getExpectedSize()))
            .addColumn(DConstants.TASK_FAMILY,
                    DConstants.TASK_TRANSFERRED_SIZE_QUALIFIER,
                    Bytes.toBytes(status.getTransferredSize()));
    return put;
  }

  @Override
  public void add(AccessStatus status) throws IOException {
    try {
      buffer.put(status);
    } catch (InterruptedException ex) {
      throw new LogIOException("Failed to add new access status to buffer");
    }
  }

  @Override
  public CloseableIterator<AccessStatus> list(final AccessStatusQuery query) throws IOException {
    FilterList filters = new FilterList();
    query.getRedirectFrom().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_REDIRECT_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(TrekUtils.wildcardToRegular(v))));
    });
    query.getServerName().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_SERVER_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(TrekUtils.wildcardToRegular(v))));
    });
    query.getClientName().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_CLIENT_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(TrekUtils.wildcardToRegular(v))));
    });
    query.getFrom().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_FROM_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(TrekUtils.wildcardToRegular(v))));
    });
    query.getTo().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_TO_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(TrekUtils.wildcardToRegular(v))));
    });
    query.getState().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_STATE_QUALIFIER,
              CompareFilter.CompareOp.EQUAL,
              v.getCode()));
    });
    query.getProgress().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_PROGRESS_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_PROGRESS_QUALIFIER,
              CompareFilter.CompareOp.LESS,
              Bytes.toBytes(v.getMaxValue())));
    });
    query.getStartTime().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_START_TIME_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_START_TIME_QUALIFIER,
              CompareFilter.CompareOp.LESS,
              Bytes.toBytes(v.getMaxValue())));
    });
    query.getElapsed().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_ELAPSED_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_ELAPSED_QUALIFIER,
              CompareFilter.CompareOp.LESS,
              Bytes.toBytes(v.getMaxValue())));
    });
    query.getExpectedSize().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_EXPECTED_SIZE_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_EXPECTED_SIZE_QUALIFIER,
              CompareFilter.CompareOp.LESS,
              Bytes.toBytes(v.getMaxValue())));
    });
    query.getTransferredSize().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_TRANSFERRED_SIZE_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.TASK_FAMILY,
              DConstants.TASK_TRANSFERRED_SIZE_QUALIFIER,
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
      List<AccessStatus> rvals = new LinkedList<>();
      try (Table table = conn.get().getTable(tableName)) {
        Result result = table.get(get);
        if (!result.isEmpty()) {
          rvals.add(toAccessStatus(result));
        }
      }
      return IteratorUtils.wrap(rvals.iterator());
    } else {
      query.getId().ifPresent(v -> {
        filters.addFilter(new RowFilter(
                CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(TrekUtils.wildcardToRegular(v))));
      });
      Scan scan = new Scan();
      Set<OrderKey<Field>> orderKeys = query.getOrderKey();
      if (orderKeys.size() == 1
              && orderKeys.stream().allMatch(v -> v.getKey() == Field.ID)) {
        scan.setReversed(!orderKeys.stream()
                .findFirst()
                .get()
                .getAsc());
      }
      scan.setCaching(DConstants.SCANNER_CACHING);
      if (!filters.getFilters().isEmpty()) {
        scan.setFilter(filters);
      }
      TableIterator tableIter = new TableIterator(conn.get().getTable(tableName), scan);
      return Optional.of(IteratorUtils.wrap(tableIter, (Result result)
              -> {
        AccessStatus status = null;
        try {
          status = toAccessStatus(result);
        } catch (TaskIOException e) {
          LOG.error(e);
        }
        return status;
      }))
              .map(iter -> IteratorUtils.wrap(iter, query.getComparator()))
              .map(iter -> IteratorUtils.wrapOffset(iter, query.getOffset()))
              .map(iter -> IteratorUtils.wrapLimit(iter, query.getLimit()))
              .map(iter -> query.getKeep()
                      ? iter
                      : IteratorUtils.wrap(iter, (List<AccessStatus> infos) -> {
                        if (infos.isEmpty()) {
                          return new ArrayList<>(0);
                        }
                        Map<String, AccessStatus> mapInfos = new HashMap<>();
                        infos.stream()
                                .filter(v -> v.getState() == TaskState.FAILED
                                        || v.getState() == TaskState.SUCCEED)
                                .forEach(v -> mapInfos.put(v.getId(), v));
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

  private static TaskIOException getCellException(final String name) {
    return new TaskIOException("The " + name + " value does not exist");
  }

  private static AccessStatus toAccessStatus(Result result) throws TaskIOException {
    String id = Bytes.toString(result.getRow());
    String redirectFrom = HBaseUtils.getAndCheckString(result.getValue(
            DConstants.TASK_FAMILY,
            DConstants.TASK_REDIRECT_QUALIFIER))
            .map(v -> Bytes.toString(v))
            .orElseThrow(() -> getCellException("refirectFrom"));
    String serverName = HBaseUtils.getAndCheckString(result.getValue(
            DConstants.TASK_FAMILY,
            DConstants.TASK_SERVER_QUALIFIER))
            .map(v -> Bytes.toString(v))
            .orElseThrow(() -> getCellException("serverName"));
    String clientName = HBaseUtils.getAndCheckString(result.getValue(
            DConstants.TASK_FAMILY,
            DConstants.TASK_CLIENT_QUALIFIER))
            .map(v -> Bytes.toString(v))
            .orElseThrow(() -> getCellException("clientName"));
    String from = HBaseUtils.getAndCheckString(result.getValue(
            DConstants.TASK_FAMILY,
            DConstants.TASK_FROM_QUALIFIER))
            .map(v -> Bytes.toString(v))
            .orElseThrow(() -> getCellException("from"));
    String to = HBaseUtils.getAndCheckString(result.getValue(
            DConstants.TASK_FAMILY,
            DConstants.TASK_TO_QUALIFIER))
            .map(v -> Bytes.toString(v))
            .orElseThrow(() -> getCellException("to"));
    TaskState state = TaskState.find(
            HBaseUtils.getAndCheckString(result.getValue(
                    DConstants.TASK_FAMILY,
                    DConstants.TASK_STATE_QUALIFIER))
            .orElseThrow(() -> getCellException("status")))
            .orElseThrow(() -> new TaskIOException("No found valid status"));
    double progress = HBaseUtils.getAndCheckDouble(result.getValue(
            DConstants.TASK_FAMILY,
            DConstants.TASK_PROGRESS_QUALIFIER))
            .map(v -> Bytes.toDouble(v))
            .orElseThrow(() -> getCellException("progress"));
    long startTime = HBaseUtils.getAndCheckDouble(result.getValue(
            DConstants.TASK_FAMILY,
            DConstants.TASK_START_TIME_QUALIFIER))
            .map(v -> Bytes.toLong(v))
            .orElseThrow(() -> getCellException("startTime"));
    long elapsed = HBaseUtils.getAndCheckDouble(result.getValue(
            DConstants.TASK_FAMILY,
            DConstants.TASK_ELAPSED_QUALIFIER))
            .map(v -> Bytes.toLong(v))
            .orElseThrow(() -> getCellException("elapsed"));
    long expectedSize = HBaseUtils.getAndCheckDouble(result.getValue(
            DConstants.TASK_FAMILY,
            DConstants.TASK_EXPECTED_SIZE_QUALIFIER))
            .map(v -> Bytes.toLong(v))
            .orElseThrow(() -> getCellException("expectedSize"));
    long transferredSize = HBaseUtils.getAndCheckDouble(result.getValue(
            DConstants.TASK_FAMILY,
            DConstants.TASK_TRANSFERRED_SIZE_QUALIFIER))
            .map(v -> Bytes.toLong(v))
            .orElseThrow(() -> getCellException("transferredSize"));
    return AccessStatus.newBuilder()
            .setId(id)
            .setRedirectFrom(redirectFrom)
            .setServerName(serverName)
            .setClientName(clientName)
            .setFrom(from)
            .setTo(to)
            .setTaskState(state)
            .setProgress(progress)
            .setStartTime(startTime)
            .setElapsed(elapsed)
            .setExpectedSize(expectedSize)
            .setTransferredSize(transferredSize)
            .build();
  }

  @Override
  public boolean supportFilter(AccessStatusQuery query) {
    return true;
  }

  @Override
  public Optional<AccessStatus> find(String id) throws IOException {
    try (Table table = conn.get().getTable(tableName)) {
      Result result = table.get(new Get(Bytes.toBytes(id)));
      if (result.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(toAccessStatus(result));
    }
  }

  @Override
  public CloseableIterator<AccessStatus> list() throws IOException {
    return list(AccessStatusQuery.QUERY_ALL);
  }
}
