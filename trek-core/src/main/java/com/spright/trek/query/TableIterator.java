package com.spright.trek.query;

import com.spright.trek.utils.TrekUtils;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;

public class TableIterator implements CloseableIterator<Result> {

  private static final Log LOG = LogFactory.getLog(TableIterator.class);
  private final Table table;
  private final ResultScanner scanner;
  private final Iterator<Result> iter;

  public TableIterator(final Table table, final Scan scan) throws IOException {
    try {
      this.table = table;
      this.scanner = table.getScanner(scan);
      this.iter = scanner.iterator();
    } catch (IOException e) {
      table.close();
      throw e;
    }
  }

  public Table getTable() {
    return table;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public Result next() {
    return iter.next();
  }

  @Override
  public void close() throws IOException {
    TrekUtils.closeWithLog(scanner, LOG);
    TrekUtils.closeWithLog(table, LOG);
  }

}
