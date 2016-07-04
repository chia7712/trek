package com.spright.trek.datasystem;

import com.spright.trek.datasystem.request.DataType;
import com.spright.trek.datasystem.request.DataInfo;
import com.spright.trek.lock.Lock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.spright.trek.datasystem.request.DataInfoQuery;
import com.spright.trek.datasystem.request.UriRequest;
import com.spright.trek.datasystem.request.ReadDataRequest;
import com.spright.trek.datasystem.request.WriteDataRequest;
import com.spright.trek.exception.UriParseIOException;
import com.spright.trek.utils.TrekUtils;
import com.spright.trek.io.InputStreamAdapter;
import com.spright.trek.io.OutputStreamAdapter;
import com.spright.trek.loadbalancer.MetricsGroup;
import com.spright.trek.loadbalancer.MetricsGroupFactory;
import com.spright.trek.loadbalancer.Operation;
import com.spright.trek.loadbalancer.UndealtMetrics;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import com.spright.trek.lock.LockManager;
import com.spright.trek.lock.LockManagerFactory;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.query.IteratorUtils;
import java.util.Optional;
import java.util.UUID;

public abstract class DataSystem {

  private static final Log LOG = LogFactory.getLog(HBaseDataSystem.class);
  private static final ConcurrentMap<Protocol, DataSystem> DATA_SYSTEM = new ConcurrentSkipListMap<>();

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      closeAll();
    }));
  }

  public static void closeAll() {
    DATA_SYSTEM.values().forEach(
            ds -> TrekUtils.closeWithLog(() -> ds.close(), LOG));
    DATA_SYSTEM.clear();
  }

  public static DataSystem getInstance(final UriRequest request, final Configuration config) throws Exception {
    Protocol protocol = request.getScheme();
    DataSystem ds = DATA_SYSTEM.computeIfAbsent(protocol, (p) -> {
      try {
        switch (p) {
          case HDFS:
            return new HdfsDataSystem(config);
          case HBASE:
            return new HBaseDataSystem(config);
          case FTP:
            return new FtpDataSystem(config);
          case SMB:
            return new SmbDataSystem(config);
          case LOCAL:
            return new LocalDataSystem(config);
          case FILE:
            return new FileDataSystem(config);
          case HTTP:
            return new HttpDataSystem(config);
          default:
            return null;
        }
      } catch (Exception e) {
        return null;
      }
    });
    if (ds == null) {
      throw new IOException("Failed to construct the data system for scheme:" + protocol);
    }
    return ds;
  }
  private final MetricsGroup metricsGroup;
  private final LockManager lockMa;
  private final Protocol protocol;
  private final Configuration config;

  public DataSystem(final Configuration config, final Protocol protocol)
          throws Exception {
    this.lockMa = LockManagerFactory.newInstance(config);
    this.protocol = protocol;
    this.config = config;
    this.metricsGroup = MetricsGroupFactory.getInstance(config);
  }

  public final boolean supportSort(final DataInfoQuery request) {
    return request.getOrderKey().isEmpty();
  }

  public boolean supportPredicate(final DataInfoQuery query) {
    return false;
  }

  protected static IOException getNonFileAndDirectoryException(final String path) {
    return new IOException("The " + path + " is not file or directory");
  }

  protected static IOException getFailedToDeleteFileException(final String path) {
    return new IOException("Failed to delete " + path);
  }

  /**
   * @param scheme
   * @return True if the uri is supported
   */
  public final boolean isSupported(final Protocol scheme) {
    return scheme == getScheme();
  }

  /**
   * Open a input channel from the remote storage.
   *
   * @param request The remote data location
   * @return Input channel
   * @throws IOException If failed to open input channel
   */
  public final InputChannel open(final ReadDataRequest request) throws IOException {
    checkAll(this, request);
    Lock lock = lockMa.getReadLock(request.toString(false));
    try {
      InputChannel channel = internalOpen(request);
      return new MetricableInputChannel(lock, channel,
              metricsGroup.newMetrics(Operation.READ, channel.getInfo().getSize()));
    } catch (IOException e) {
      lock.close();
      throw e;
    }
  }

  protected static String createTmpPath(final String oriPath) {
    String extension = UUID.randomUUID().toString().replaceAll("-", "");
    if (extension.length() > 10) {
      extension = extension.substring(0, 10);
    }
    return oriPath + extension;
  }

  private static void checkAll(final DataSystem fs, final UriRequest request)
          throws IOException {
    checkScheme(fs, request.getScheme());
  }

  /**
   * Create the remote data channel.
   *
   * @param request The remote data location
   * @return Output channel
   * @throws IOException If failed to open output channel
   */
  public final OutputChannel create(final WriteDataRequest request) throws IOException {
    checkAll(this, request);
    Lock lock = lockMa.getReadLock(request.toString(false));
    try {
      OutputChannel channel = internalCreate(request);
      return new MetricableOutputChannel(lock, channel,
              metricsGroup.newMetrics(Operation.WRITE));
    } catch (IOException e) {
      lock.close();
      throw e;
    }
  }

  /**
   * Lists the remote data.
   *
   * @param query The remote directory
   * @return
   * @throws IOException If failed to list remote data
   */
  public final CloseableIterator<DataInfo> list(final DataInfoQuery query) throws IOException {
    checkScheme(this, query.getScheme());
    try (UndealtMetrics metric = metricsGroup.newMetrics(Operation.LIST)) {
      return Optional.of(internalList(query))
              .map(v -> supportPredicate(query) ? v : IteratorUtils.wrap(v, query.getPredicate()))
              .map(v -> supportSort(query) ? v : IteratorUtils.wrap(v, query.getComparator()))
              .map(v -> IteratorUtils.wrapOffset(v, query.getOffset()))
              .map(v -> IteratorUtils.wrapLimit(v, query.getLimit()))
              .map(v -> query.getKeep()
                      ? v
                      : IteratorUtils.wrap(v, (DataInfo info) -> {
                        DataInfo rval = null;
                        try {
                          internalDelete(info);
                        } catch (IOException e) {
                          rval = info;
                        }
                        return rval;
                      }))
              .get();
    }
  }

  protected static long getInputSizeOrThrow(final WriteDataRequest request) throws UriParseIOException {
    return request.getExpectedSize().orElseThrow(() -> new UriParseIOException("No found of input size"));
  }

  protected static String getHostOrThrow(final UriRequest request) throws UriParseIOException {
    return request.getAccountInfo().getHost().orElseThrow(() -> new UriParseIOException(
            "No found of host:" + request.toString()));
  }

  protected static void checkDataType(final DataType expected, final DataType actually) throws UriParseIOException {
    if (expected != actually) {
      throw new UriParseIOException("expected:" + expected + " but was:" + actually);
    }
  }

  protected static void checkDataExisted(final UriRequest request, final boolean exist) throws UriParseIOException {
    if (!exist) {
      throw new UriParseIOException(request.toString() + " is not existed");
    }
  }

  private static void checkScheme(final DataSystem ds, final Protocol scheme) {
    if (!ds.isSupported(scheme)) {
      String msg = "Not supported scheme : "
              + scheme + " by " + ds.getClass().getName();
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
  }

  /**
   * @return The supported protocol
   */
  public final Protocol getScheme() {
    return protocol;
  }

  protected abstract void close() throws Exception;

  protected abstract InputChannel internalOpen(final ReadDataRequest request) throws IOException;

  protected abstract OutputChannel internalCreate(final WriteDataRequest request) throws IOException;

  protected abstract void internalDelete(final DataInfo info) throws IOException;

  /**
   * Collects all valid data info. This method invokes the
   *
   * @param request request
   * @return
   * @throws IOException If failed to list data info
   */
  protected abstract CloseableIterator<DataInfo> internalList(final DataInfoQuery request) throws IOException;

  protected Configuration getConfiguration() {
    return config;
  }

  private static final class MetricableOutputChannel implements OutputChannel {

    private final OutputChannel output;
    private final Lock lock;
    private final UndealtMetrics metrics;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    MetricableOutputChannel(final Lock lock, final OutputChannel output,
            final UndealtMetrics metrics) {
      this.output = output;
      this.lock = lock;
      this.metrics = metrics;
    }

    @Override
    public WriteDataRequest getRequest() {
      return output.getRequest();
    }

    @Override
    public OutputStream getOutputStream() {
      return new OutputStreamAdapter(output.getOutputStream()) {
        @Override
        public void write(int v) throws IOException {
          metrics.addBytes(1);
          super.write(v);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          metrics.addBytes(len);
          super.write(b, off, len);
        }
      };
    }

    @Override
    public void recover() throws IOException {
      output.recover();
    }

    @Override
    public void close() throws Exception {
      if (closed.compareAndSet(false, true)) {
        TrekUtils.closeWithLog(output, LOG);
        TrekUtils.closeWithLog(metrics, LOG);
        TrekUtils.closeWithLog(lock, LOG);
      }
    }
  }

  private static final class MetricableInputChannel implements InputChannel {

    private final InputChannel input;
    private final Lock lock;
    private final UndealtMetrics metrics;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    MetricableInputChannel(final Lock lock, final InputChannel input,
            final UndealtMetrics metrics) {
      this.input = input;
      this.lock = lock;
      this.metrics = metrics;
    }

    @Override
    public DataInfo getInfo() {
      return input.getInfo();
    }

    @Override
    public InputStream getInputStream() {
      return new InputStreamAdapter(input.getInputStream()) {
        @Override
        public int read() throws IOException {
          int rval = super.read();
          if (rval != -1) {
            metrics.addBytes(1);
          }
          return rval;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
          int rval = super.read(b, off, len);
          if (rval != -1) {
            metrics.addBytes(rval);
          }
          return rval;
        }
      };
    }

    @Override
    public void close() throws Exception {
      if (closed.compareAndSet(false, true)) {
        TrekUtils.closeWithLog(input, LOG);
        TrekUtils.closeWithLog(metrics, LOG);
        TrekUtils.closeWithLog(lock, LOG);
      }
    }
  }
}
