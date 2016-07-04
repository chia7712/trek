package com.spright.trek.datasystem;

import com.spright.trek.datasystem.request.DataType;
import com.spright.trek.datasystem.request.DataInfo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import com.spright.trek.datasystem.request.DataInfoQuery;
import com.spright.trek.datasystem.request.DataPath;
import com.spright.trek.datasystem.request.ReadDataRequest;
import com.spright.trek.datasystem.request.WriteDataRequest;
import com.spright.trek.exception.HdsIOException;
import com.spright.trek.exception.UriParseIOException;
import com.spright.trek.DConstants;
import com.spright.trek.utils.HBaseUtils;
import com.spright.trek.utils.TrekUtils;
import com.spright.trek.io.InfiniteAccesser;
import com.spright.trek.mapping.AccountInfo;
import com.spright.trek.thread.ShareableObject;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableConfiguration;
import org.apache.hadoop.hbase.filter.FilterList;
import com.spright.trek.datasystem.request.UriRequest;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.query.IteratorUtils;
import com.spright.trek.query.TableIterator;

public class HBaseDataSystem extends DataSystem {

  private static final Log LOG = LogFactory.getLog(HBaseDataSystem.class);
  private static final Set<TableName> TABLENAME_CACHE = new TreeSet<>();
  private final Path rootPath;
  private final FileSystem fs;
  private final int limit;
  private final int initialCap;
  private final String namespace;
  private final ShareableObject<Connection> conn;

  public HBaseDataSystem(final Configuration conf) throws Exception {
    super(conf, Protocol.HBASE);
    fs = FileSystem.get(conf);
    namespace = conf.get(DConstants.TREK_NAMESPACE, DConstants.DEFAULT_TREK_NAMESPACE);
    conn = ShareableObject.create(Connection.class,
            () -> ConnectionFactory.createConnection(conf));

    try (Admin admin = conn.get().getAdmin()) {
      TrekUtils.chekcHBaseNamespace(admin, namespace);
      synchronized (TABLENAME_CACHE) {
        TABLENAME_CACHE.addAll(Arrays.asList(
                admin.listTableNamesByNamespace(namespace)));
      }
    } catch (IOException e) {
      conn.close();
      throw e;
    }
    limit = Math.min(
            conf.getInt(TableConfiguration.MAX_KEYVALUE_SIZE_KEY, Integer.MAX_VALUE),
            conf.getInt(DConstants.TREK_LARGE_DATA_THRESHOLD,
                    DConstants.DEFAULT_TREK_LARGE_DATA_THRESHOLD));
    initialCap = conf.getInt(DConstants.DATA_BUFFER_INITIAL_SIZE,
            DConstants.DEFAULT_DATA_BUFFER_INITIAL_SIZE);
    InfiniteAccesser.checkMemorySetting(limit, initialCap);
    rootPath = new Path(conf.get(DConstants.HDS_ROOT_PATH,
            DConstants.DEFAULT_HDS_ROOT_PATH));
  }

  @Override
  public boolean supportPredicate(final DataInfoQuery request) {
    return true;
  }

  private void checkTableExisted(final TableName name) throws IOException {
    synchronized (TABLENAME_CACHE) {
      if (TABLENAME_CACHE.contains(name)) {
        return;
      }
      try (Admin admin = conn.get().getAdmin()) {
        if (admin.tableExists(name)) {
          TABLENAME_CACHE.add(name);
        } else {
          admin.createTable(getHTableDescriptor(name));
          TABLENAME_CACHE.add(name);
        }
      }
    }
  }

  private static HTableDescriptor getHTableDescriptor(final TableName name) {
    HTableDescriptor table = new HTableDescriptor(name);
    HColumnDescriptor columnDesc = new HColumnDescriptor(DConstants.DEFAULT_FAMILY);
    columnDesc.setCompressionType(Compression.Algorithm.GZ);
    columnDesc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    columnDesc.setMaxVersions(1);
    table.addFamily(columnDesc);
    return table;
  }

  private static Get newGetWithAllQualifier(final UriRequest request) throws IOException {
    Get get = new Get(Bytes.toBytes(request.getPath().getName()));
    get.addFamily(DConstants.HDS_TABLE_FAMILY);
    return get;
  }

  @Override
  public InputChannel internalOpen(final ReadDataRequest request) throws IOException {
    final TableName tableName = checkLegalCatalog(namespace, request.getPath().getCatalog());
    try (Table table = conn.get().getTable(tableName)) {
      Record record = Record.valueOf(this, tableName, table.get(newGetWithAllQualifier(request)));
      return new InputChannel() {
        private final InputStream input = record.getInputStream();

        @Override
        public void close() throws IOException {
          input.close();
        }

        @Override
        public DataInfo getInfo() {
          return record;
        }

        @Override
        public InputStream getInputStream() {
          return input;
        }
      };
    } catch (TableNotFoundException e) {
      throw new HdsIOException("No found of table: " + tableName);
    }
  }

  private static Get newGetWithoutHBaseValue(final byte[] key) {
    Get get = new Get(key);
    get.addColumn(DConstants.HDS_TABLE_FAMILY, DConstants.HDS_DATA_UPLOAD_TIME_QUALIFIER);
    get.addColumn(DConstants.HDS_TABLE_FAMILY, DConstants.HDS_DATA_SIZE_QUALIFIER);
    get.addColumn(DConstants.HDS_TABLE_FAMILY, DConstants.HDS_DATA_LINK_QUALIFIER);
    return get;
  }

  private static TableName checkLegalCatalog(final String namespace,
          String catalog) throws HdsIOException, UriParseIOException {
    if (catalog.startsWith("/")) {
      catalog = catalog.substring(1);
    }
    if (catalog.contains("/")) {
      throw new UriParseIOException("Invalid catalog: " + catalog);
    }
    try {
      return TableName.valueOf(namespace, catalog);
    } catch (IllegalArgumentException e) {
      throw new HdsIOException("Invalid catalog name: " + catalog);
    }
  }

  @Override
  protected void internalDelete(final DataInfo info) throws IOException {
    if (info instanceof Record) {
      Record record = (Record) info;
      TableName tableName = checkLegalCatalog(namespace, record.getUriRequest().getPath().getCatalog());
      try (Table table = conn.get().getTable(tableName)) {
        record.delete(table);
      }
    } else {
      String msg = "The implemenation of DataInfo should be Recode not "
              + info.getClass().getName();
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  private static Path createPath(final Path rootPath, final String catalog,
          final String name) throws IOException {
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(System.currentTimeMillis());
    int year = cal.get(Calendar.YEAR);
    String month;
    if (cal.get(Calendar.MONTH) < 10) {
      month = "0" + cal.get(Calendar.MONTH);
    } else {
      month = String.valueOf(Calendar.MONTH);
    }
    StringBuilder str = new StringBuilder();
    str.append(catalog)
            .append("/")
            .append(year)
            .append(month)
            .append("/")
            .append(name);
    return new Path(rootPath, str.toString());
  }

  private static Path getTmpPath(final FileSystem fs, final Path realPath)
          throws IOException {
    if (realPath == null) {
      return null;
    } else {
      Path tmp = new Path(realPath.toString() + DConstants.DATA_TMP_EXTENSION);
      while (fs.exists(tmp)) {
        tmp = new Path(tmp.toString() + DConstants.DATA_TMP_EXTENSION);
      }
      return tmp;
    }
  }

  @Override
  public OutputChannel internalCreate(final WriteDataRequest request) throws IOException {
    final TableName tableName = checkLegalCatalog(namespace, request.getPath().getCatalog());
    checkTableExisted(tableName);
    final long ts = request.getExpectedTime().orElse(System.currentTimeMillis());
    Path realPath = createPath(rootPath, tableName.getQualifierAsString(), request.getPath().getName());
    Path tmpPath = new Path(createTmpPath(realPath.toString()));
    InfiniteAccesser infiniteAccess = new InfiniteAccesser(limit, initialCap,
            new InfiniteAccesser.HdfsIOFactory(fs, tmpPath));
    return new OutputChannel() {
      private final OutputStream output = infiniteAccess.getOutputStream();
      private final byte[] rowKey = Bytes.toBytes(request.getPath().getName());
      private boolean hasPutToHBase = false;
      private boolean hasMove = false;

      @Override
      public WriteDataRequest getRequest() {
        return request;
      }

      @Override
      public void close() throws IOException {
        output.close();
        Put put = new Put(rowKey);
        put.addColumn(DConstants.HDS_TABLE_FAMILY,
                DConstants.HDS_DATA_UPLOAD_TIME_QUALIFIER,
                ts,
                Bytes.toBytes(ts))
                .addColumn(DConstants.HDS_TABLE_FAMILY,
                        DConstants.HDS_DATA_SIZE_QUALIFIER,
                        ts,
                        Bytes.toBytes(infiniteAccess.getSize()));
        if (infiniteAccess.isInMemory()) {
          put.add(createHBaseValueCell(put.getRow(), ts,
                  infiniteAccess.getBuffer(),
                  infiniteAccess.getBufferOffset(),
                  infiniteAccess.getBufferLength()));
        } else {
          boolean exist = fs.exists(realPath);
          if (exist && !fs.isFile(realPath)) {
            throw new IOException("The dst is existed and it is not file");
          }
          if (exist && !fs.delete(realPath, false)) {
            throw new IOException("Failed to delete " + realPath);
          }
          hasMove = fs.rename(tmpPath, realPath);
          if (!hasMove) {
            throw new HdsIOException("Failed to move "
                    + tmpPath + " to " + realPath);
          }
          put.addColumn(DConstants.HDS_TABLE_FAMILY,
                  DConstants.HDS_DATA_LINK_QUALIFIER,
                  ts,
                  Bytes.toBytes(realPath.toString()));
        }
        try (Table t = conn.get().getTable(tableName)) {
          Result result = t.get(newGetWithoutHBaseValue(put.getRow()));
          byte[] link = result.getValue(DConstants.HDS_TABLE_FAMILY,
                  DConstants.HDS_DATA_LINK_QUALIFIER);
          if (link != null) {
            Path previousPath = new Path(Bytes.toString(link));
            fs.delete(previousPath, false);
          }
          t.put(put);
        }
        hasPutToHBase = true;
      }

      @Override
      public void recover() throws IOException {
        if (!infiniteAccess.isInMemory()) {
          if (hasMove) {
            TrekUtils.closeWithLog(() -> fs.delete(realPath, true), LOG);
          } else {
            TrekUtils.closeWithLog(() -> fs.delete(tmpPath, true), LOG);
          }
        }
        if (hasPutToHBase) {
          try (Table t = conn.get().getTable(tableName)) {
            t.delete(new Delete(rowKey));
          }
        }
      }

      @Override
      public OutputStream getOutputStream() {
        return output;
      }
    };
  }

  private static Cell createHBaseValueCell(final byte[] row, final long ts,
          final byte[] buf, final int offset, final int len) {
    return new KeyValue(
            row, 0, row.length,
            DConstants.HDS_TABLE_FAMILY, 0, DConstants.HDS_TABLE_FAMILY.length,
            DConstants.HDS_DATA_CONTENT_QUALIFIER, 0, DConstants.HDS_DATA_CONTENT_QUALIFIER.length,
            ts, KeyValue.Type.Put,
            buf, offset, len);
  }

  @Override
  protected void close() throws Exception {
    conn.close();
  }

  @Override
  public CloseableIterator<DataInfo> internalList(final DataInfoQuery request) throws IOException {
    final TableName tableName = checkLegalCatalog(namespace, request.getPath().getCatalog());
    if (!request.getTypes().stream().anyMatch(v -> v == DataType.FILE)) {
      return IteratorUtils.wrap(new ArrayList<DataInfo>(0).iterator());
    }
    FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    request.getTime().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.HDS_TABLE_FAMILY,
              DConstants.HDS_DATA_UPLOAD_TIME_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.HDS_TABLE_FAMILY,
              DConstants.HDS_DATA_UPLOAD_TIME_QUALIFIER,
              CompareFilter.CompareOp.LESS,
              Bytes.toBytes(v.getMaxValue())));
    });
    request.getSize().ifPresent(v -> {
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.HDS_TABLE_FAMILY,
              DConstants.HDS_DATA_SIZE_QUALIFIER,
              CompareFilter.CompareOp.GREATER_OR_EQUAL,
              Bytes.toBytes(v.getMinValue())));
      filters.addFilter(new SingleColumnValueFilter(
              DConstants.HDS_TABLE_FAMILY,
              DConstants.HDS_DATA_SIZE_QUALIFIER,
              CompareFilter.CompareOp.LESS,
              Bytes.toBytes(v.getMaxValue())));
    });
    filters.getFilters().stream()
            .filter(v -> v instanceof SingleColumnValueFilter)
            .forEach(v -> ((SingleColumnValueFilter) v).setFilterIfMissing(true));

    if (TrekUtils.hasWildcardToRegular(request.getPath().getName())) {
      Scan scan = new Scan();
      filters.addFilter(new RowFilter(
              CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(TrekUtils.wildcardToRegular(
                      request.getPath().getName()))));
      scan.setCaching(DConstants.SCANNER_CACHING);
      scan.addColumn(DConstants.HDS_TABLE_FAMILY,
              DConstants.HDS_DATA_UPLOAD_TIME_QUALIFIER)
              .addColumn(DConstants.HDS_TABLE_FAMILY,
                      DConstants.HDS_DATA_SIZE_QUALIFIER)
              .addColumn(DConstants.HDS_TABLE_FAMILY,
                      DConstants.HDS_DATA_LINK_QUALIFIER);
      if (!filters.getFilters().isEmpty()) {
        scan.setFilter(filters);
      }
      return IteratorUtils.wrap(new TableIterator(conn.get().getTable(tableName), scan),
              (Result result) -> {
                DataInfo info = null;
                try {
                  info = Record.valueOf(HBaseDataSystem.this, tableName, result);
                } catch (IOException e) {
                  LOG.error(e);
                }
                return info;
              });
    } else {
      Get get = new Get(Bytes.toBytes(request.getPath().getName()))
              .addColumn(DConstants.HDS_TABLE_FAMILY,
                      DConstants.HDS_DATA_UPLOAD_TIME_QUALIFIER)
              .addColumn(DConstants.HDS_TABLE_FAMILY,
                      DConstants.HDS_DATA_SIZE_QUALIFIER)
              .addColumn(DConstants.HDS_TABLE_FAMILY,
                      DConstants.HDS_DATA_LINK_QUALIFIER);
      if (!filters.getFilters().isEmpty()) {
        get.setFilter(filters);
      }
      try (Table rable = conn.get().getTable(tableName)) {
        Result result = rable.get(get);
        List<DataInfo> rval = new ArrayList<>(1);
        if (!result.isEmpty()) {
          rval.add(Record.valueOf(HBaseDataSystem.this, tableName, result));
        }
        return IteratorUtils.wrap(rval.iterator());
      }
    }
  }

  private static DataInfo toDataInfo(final HBaseDataSystem ds, final TableName tableName, final Result result) throws IOException {
    DataInfo.Builder builder = DataInfo.newBuilder()
            .setRequest(new UriRequest(Protocol.HBASE,
                    AccountInfo.EMPTY,
                    new DataPath("/" + tableName.getQualifierAsString(), Bytes.toString(result.getRow()))))
            .setType(DataType.FILE);
    Path hdfsPath = HBaseUtils.getAndCheckString(result.getValue(DConstants.HDS_TABLE_FAMILY,
            DConstants.HDS_DATA_LINK_QUALIFIER)).map(v -> new Path(Bytes.toString(v))).orElse(null);
    if (hdfsPath != null) {
      builder.setOwners(TrekUtils.findOwners(ds.fs, hdfsPath));
    } else {
      builder.setOwners(TrekUtils.findOwners(ds.conn.get(), tableName, result.getRow()));
      hdfsPath = null;
    }
    builder.setSize(HBaseUtils.getAndCheckLong(result.getValue(DConstants.HDS_TABLE_FAMILY,
            DConstants.HDS_DATA_SIZE_QUALIFIER)).map(v -> Bytes.toLong(v))
            .orElseThrow(() -> new HdsIOException("No found of size parameter")))
            .setUploadTime(HBaseUtils.getAndCheckLong(result.getValue(DConstants.HDS_TABLE_FAMILY,
                    DConstants.HDS_DATA_UPLOAD_TIME_QUALIFIER)).map(v -> Bytes.toLong(v))
                    .orElseThrow(() -> new HdsIOException("No found of timestamp parameter")));
    return builder.build();
  }

  private static class Record extends DataInfo {

    private final Path hdfsPath;
    private final Result result;
    private final FileSystem fs;

    public static Record valueOf(final HBaseDataSystem ds, final TableName tableName, final Result result) throws HdsIOException, IOException {
      DataInfo.Builder builder = DataInfo.newBuilder()
              .setRequest(new UriRequest(Protocol.HBASE,
                      AccountInfo.EMPTY,
                      new DataPath("/" + tableName.getQualifierAsString(), Bytes.toString(result.getRow()))))
              .setType(DataType.FILE);
      Path hdfsPath = HBaseUtils.getAndCheckString(result.getValue(DConstants.HDS_TABLE_FAMILY,
              DConstants.HDS_DATA_LINK_QUALIFIER)).map(v -> new Path(Bytes.toString(v))).orElse(null);
      if (hdfsPath != null) {
        builder.setOwners(TrekUtils.findOwners(ds.fs, hdfsPath));
      } else {
        builder.setOwners(TrekUtils.findOwners(ds.conn.get(), tableName, result.getRow()));
        hdfsPath = null;
      }
      builder.setSize(HBaseUtils.getAndCheckLong(result.getValue(DConstants.HDS_TABLE_FAMILY,
              DConstants.HDS_DATA_SIZE_QUALIFIER)).map(v -> Bytes.toLong(v))
              .orElseThrow(() -> new HdsIOException("No found of size parameter")))
              .setUploadTime(HBaseUtils.getAndCheckLong(result.getValue(DConstants.HDS_TABLE_FAMILY,
                      DConstants.HDS_DATA_UPLOAD_TIME_QUALIFIER)).map(v -> Bytes.toLong(v))
                      .orElseThrow(() -> new HdsIOException("No found of timestamp parameter")));
      return new Record(builder.build(), hdfsPath, result, ds.fs);
    }

    Record(final DataInfo info, final Path hdfsPath, final Result result, final FileSystem fs) {
      super(info);
      this.result = result;
      this.hdfsPath = hdfsPath;
      this.fs = fs;
    }

    void delete(final Table table) throws IOException {
      if (hdfsPath != null) {
        fs.delete(hdfsPath, true);
      }
      table.delete(new Delete(result.getRow()));
    }

    InputStream getInputStream() throws IOException {
      if (hdfsPath != null) {
        return fs.open(hdfsPath);
      } else {
        Cell cell = result.getColumnLatestCell(DConstants.HDS_TABLE_FAMILY,
                DConstants.HDS_DATA_CONTENT_QUALIFIER);
        if (cell == null) {
          throw new HdsIOException("No found of data in hbase cell");
        }
        return new ByteArrayInputStream(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
      }
    }
  }
}
