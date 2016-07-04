package com.spright.trek.utils;

import com.spright.trek.query.RangeNumber;
import com.spright.trek.datasystem.request.DataOwner;
import com.spright.trek.exception.UriParseIOException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.logging.Log;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

public class TrekUtils {

  private TrekUtils() {
  }
  private static final String LOCAL_HOSTNAME;
  private static final String LOCAL_ADDRESS;

  static {
    try {
      LOCAL_HOSTNAME = InetAddress.getLocalHost().getHostName();
      LOCAL_ADDRESS = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException("Failed to get local hostname", e);
    }
  }

  public static boolean hasWildcardToRegular(final String wildcard) {
    return wildcard.contains("?") || wildcard.contains("*");
  }

  public static String wildcardToRegular(final String wildcard) {
    String regular = wildcard.replaceAll("\\?", "\\.").replaceAll("\\*", "\\.*");
    if (!regular.startsWith(".*")) {
      regular = "^" + regular;
    }
    if (!regular.endsWith("*")) {
      regular = regular + "$";
    }
    return regular;
  }

  public static String decode(final String v) throws UriParseIOException {
    try {
      return URLDecoder.decode(v, "UTF-8");
    } catch (UnsupportedEncodingException | IllegalArgumentException ex) {
      throw new UriParseIOException("Failed to decode string :" + v, ex);
    }
  }

  public static String getAddress() {
    return LOCAL_ADDRESS;
  }

  public static String getHostname() {
    return LOCAL_HOSTNAME;
  }

  public static List<DataOwner> findOwners(FileSystem fs, Path path) throws IOException {
    return findOwners(fs, fs.getFileStatus(path));
  }

  public static List<DataOwner> findOwners(FileSystem fs, final FileStatus status) throws IOException {
    BlockLocation[] blks = fs.getFileBlockLocations(status, 0, status.getLen());
    Map<String, AtomicInteger> hostCount = new TreeMap();
    if (blks != null) {
      for (BlockLocation blk : blks) {
        for (String host : blk.getHosts()) {
          AtomicInteger count = hostCount.get(host);
          if (count == null) {
            count = new AtomicInteger(0);
            hostCount.put(host, count);
          }
          count.incrementAndGet();
        }
      }
    }
    if (hostCount.isEmpty()) {
      return Collections.emptyList();
    }
    List<Map.Entry<String, AtomicInteger>> mapList = new LinkedList(hostCount.entrySet());
    Collections.sort(mapList, (Map.Entry<String, AtomicInteger> o1, Map.Entry<String, AtomicInteger> o2) -> {
      return Integer.compare(o2.getValue().get(), o1.getValue().get());
    });
    final long sum = mapList.stream().mapToLong(entry -> entry.getValue().get()).sum();
    if (sum == 0) {
      return Collections.emptyList();
    }
    return mapList.stream()
            .map(entry -> new DataOwner(entry.getKey(),
                    (double) entry.getValue().get() / (double) sum))
            .collect(Collectors.toList());
  }

  public static List<DataOwner> findOwners(final Connection conn,
          final TableName tableName, final byte[] key) throws IOException {
    List<DataOwner> hosts = new ArrayList<>();
    if (conn instanceof ClusterConnection) {
      ClusterConnection cConn = (ClusterConnection) conn;
      HRegionLocation regionLoc = cConn.locateRegion(tableName, key);
      if (regionLoc != null) {
        hosts.add(new DataOwner(regionLoc.getHostname(), 1.0f));
      }
    } else {
      try (Admin admin = conn.getAdmin()) {
        final ClusterStatus cluster = admin.getClusterStatus();
        HRegionInfo region = null;
        for (HRegionInfo r : admin.getTableRegions(tableName)) {
          if (Bytes.compareTo(r.getStartKey(), key) <= 0
                  && (r.getEndKey().length == 0
                  || Bytes.compareTo(key, r.getEndKey()) < 0)) {
            region = r;
            break;
          }
        }
        if (region == null) {
          return hosts;
        }
        for (ServerName n : cluster.getServers()) {
          ServerLoad load = cluster.getLoad(n);
          for (RegionLoad rl : load.getRegionsLoad().values()) {
            if (Bytes.compareTo(rl.getName(), region.getRegionName()) == 0) {
              hosts.add(new DataOwner(n.getHostname(), 1.0f));
              break;
            }
          }
        }
      }
    }
    return hosts;
  }

  public static void chekcHBaseNamespace(final Admin admin, final String namespace) throws IOException {
    try {
      boolean hasNamespace = Stream.of(admin.listNamespaceDescriptors())
              .anyMatch(name -> name.getName().equals(namespace));
      if (!hasNamespace) {
        admin.createNamespace(NamespaceDescriptor.create(namespace).build());
      }
    } finally {
      admin.close();
    }
  }

  public static void chekcHBaseNamespace(final Connection conn, final String namespace) throws IOException {
    chekcHBaseNamespace(conn.getAdmin(), namespace);
  }

  /**
   * /**
   * Close object without exception.
   *
   * @param closable The object to close
   * @param log logs the exception message
   * @return Exception
   */
  public static Optional<Exception> closeWithLog(final AutoCloseable closable,
          final Log log) {
    if (closable == null) {
      return Optional.empty();
    }
    try {
      closable.close();
      return Optional.empty();
    } catch (Exception e) {
      if (log != null) {
        log.error("Failed to close object", e);
      }
      return Optional.of(e);
    }
  }
}
