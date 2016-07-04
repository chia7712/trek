package com.spright.trek.web;

import com.google.gson.stream.JsonWriter;
import com.spright.trek.DConstants;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.spright.trek.io.AtomicCloseable;
import com.spright.trek.datasystem.request.DataInfo;
import com.spright.trek.datasystem.DataSystem;
import com.spright.trek.datasystem.InputChannel;
import com.spright.trek.datasystem.OutputChannel;
import com.spright.trek.datasystem.Protocol;
import com.spright.trek.datasystem.request.DataInfoQuery;
import com.spright.trek.datasystem.request.ReadDataRequest;
import com.spright.trek.datasystem.request.WriteDataRequest;
import com.spright.trek.exception.UriParseIOException;
import com.spright.trek.io.json.JsonIO;
import com.spright.trek.mapping.AccountInfo;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.spright.trek.mapping.AccountInfoUpdate;
import com.spright.trek.task.AccessStatusQuery;
import com.spright.trek.mapping.AccountInfoQuery;
import com.spright.trek.mapping.Mapping;
import com.spright.trek.mapping.MappingFactory;
import com.spright.trek.query.CloseableIterator;
import java.util.concurrent.Executors;
import org.apache.hadoop.hbase.HBaseConfiguration;
import com.spright.trek.task.AccessStatus;
import com.spright.trek.task.AccessTask;
import com.spright.trek.task.AccessTaskLogger;
import com.spright.trek.utils.TrekUtils;
import com.spright.trek.task.AccessTaskRequest;
import com.spright.trek.task.AccessTaskExecutor;
import com.spright.trek.task.AccessTaskExecutorFactory;
import com.spright.trek.task.AccessTaskLoggerFactory;
import com.spright.trek.web.MethodHandler.JsonResult;

public class WebServer extends AtomicCloseable {

  public static void main(String[] args) throws Exception {
    WebServer server = new WebServer(HBaseConfiguration.create());
  }
  private static final Log LOG = LogFactory.getLog(WebServer.class);
  private static final UriParseIOException UNKNOWN_ACTION = new UriParseIOException("Unknown dataserver action");
  private final HttpServer server;
  private final AccessTaskExecutor executor;
  private final Configuration config;
  private final Mapping mapping;
  private final AccessTaskLogger taskLogger;
  private final boolean enableGzip;
  private final int jsonCapacity;
  private final int operationBatch;

  public WebServer(final Configuration conf) throws Exception {
    int threads = conf.getInt(DConstants.RESTFUL_SERVER_CONNECTION_NUMBER,
            DConstants.DEFAULT_RESTFUL_SERVER_CONNECTION_NUMBER);
    server = HttpServer.create(new InetSocketAddress(
            conf.get(DConstants.RESTFUL_SERVER_BINDING_ARRRESS, DConstants.DEFAULT_RESTFUL_SERVER_BINDING_ARRRESS),
            conf.getInt(DConstants.RESTFUL_SERVER_BINDING_PORT, DConstants.DEFAULT_RESTFUL_SERVER_BINDING_PORT)),
            threads);
    config = conf;
    taskLogger = AccessTaskLoggerFactory.instance(conf);
    executor = AccessTaskExecutorFactory.newInstance(conf, taskLogger);
    mapping = MappingFactory.newInstance(conf);
    jsonCapacity = config.getInt(DConstants.JSON_BUFFER_LIMIT,
            DConstants.DEFAULT_JSON_BUFFER_LIMIT);
    operationBatch = config.getInt(DConstants.OPERATION_BATCH_LIMIT,
            DConstants.DEFAULT_OPERATION_BATCH_LIMIT);
    enableGzip = conf.getBoolean(DConstants.ENABLE_GZIP_RESPONSE, DConstants.DEFAULT_ENABLE_GZIP_RESPONSE);
    server.createContext("/trek", (HttpExchange he) -> {
      URI uri = he.getRequestURI();
      LOG.info("accept uri:" + uri);
      MethodHandler handler = newMethodHandler(uri.getPath());
      handler.handle(this, he);
    });
    server.setExecutor(Executors.newFixedThreadPool(threads));
    server.start();
  }

  public boolean enableGzip() {
    return enableGzip;
  }

  public AccessTaskLogger getLogger() {
    return taskLogger;
  }

  public int getJsonCapacity() {
    return jsonCapacity;
  }

  public String getBindingAddress() {
    return server.getAddress().getHostName();
  }

  public int getBindingPort() {
    return server.getAddress().getPort();
  }

  public AccessTaskExecutor getTaskExecutor() {
    return executor;
  }

  public int getOperationBatch() {
    return operationBatch;
  }

  public Configuration getConfiguration() {
    return config;
  }

  public Mapping getMapping() {
    return mapping;
  }

  @Override
  protected void internalClose() throws IOException {
    DataSystem.closeAll();
    server.stop(0);
  }

  private static MethodHandler newMethodHandler(final String path) {
    MethodHandler.Worker worker;
    if (path.contains("/mapping/update")) {
      worker = (WebServer server, HttpExchange he) -> {
        AccountInfoUpdate update = AccountInfoUpdate.parse(parseQuery(he.getRequestURI().getQuery()));
        AccountInfo info = server.getMapping().add(update);
        JsonIO json = new JsonIO(server.getJsonCapacity(), server.enableGzip());
        try (JsonWriter writer = json.getWriter()) {
          AccountInfo.write(writer, info);
        }
        return new JsonResult(HttpStatusCode.OK, json);
      };
    } else if (path.contains("/mapping/list")) {
      worker = (WebServer server, HttpExchange he) -> {
        AccountInfoQuery query = AccountInfoQuery.parse(parseQuery(he.getRequestURI().getQuery()));
        JsonIO json = new JsonIO(server.getJsonCapacity(), server.enableGzip());
        try (CloseableIterator<AccountInfo> iter = server.getMapping().list(query);
                JsonWriter writer = json.getWriter()) {
          while (iter.hasNext()) {
            AccountInfo.write(writer, iter.next());
          }
        }
        return new JsonResult(HttpStatusCode.OK, json);
      };
    } else if (path.contains("/data/access")) {
      worker = (WebServer server, HttpExchange he) -> {
        InputChannel iChannel = null;
        OutputChannel oChannel = null;
        boolean allDone = false;
        try {
          boolean needReturnJSON = true;
          Map<String, String> rawQuery = parseQuery(he.getRequestURI().getQuery());
          Optional<String> redirectFrom = Optional.ofNullable(rawQuery.get(DConstants.URI_DATA_REDIRECT_FROM));
          boolean async = Optional.ofNullable(rawQuery.get(DConstants.URI_DATA_ASYNC))
                  .map(v -> Boolean.valueOf(v))
                  .orElse(DConstants.DEFAULT_URI_DATA_ASYNC);
          ReadDataRequest readRequest = ReadDataRequest.parse(rawQuery, server.getMapping());
          LOG.info("get \"FROM\" request:" + readRequest.toString());
          if (readRequest.getScheme() == Protocol.LOCAL) {
            readRequest.attach(he);
          }
          DataSystem fromDS = DataSystem.getInstance(readRequest, server.getConfiguration());
          iChannel = fromDS.open(readRequest);
          System.out.println("read from " + fromDS.getScheme() + ":" + iChannel.getInfo().getSize());
          WriteDataRequest writeRequest = WriteDataRequest.parse(rawQuery, server.getMapping(), iChannel.getInfo());
          LOG.info("get \"TO\" request:" + writeRequest.toString());
          if (readRequest.getScheme() == Protocol.LOCAL
                  && writeRequest.getScheme() == Protocol.LOCAL) {
            throw new UriParseIOException("Unsupport to transfer data from \"LOCAL\" to \"LOCAL\"");
          }
          DataSystem toDS = DataSystem.getInstance(writeRequest, server.getConfiguration());
          if (writeRequest.getScheme() == Protocol.LOCAL) {
            needReturnJSON = false;
            async = false;
            writeRequest.attach(he);
          }
          oChannel = toDS.create(writeRequest);
          AccessTaskRequest request = AccessTaskRequest.newBuilder()
                  .setInput(iChannel)
                  .setOutput(oChannel)
                  .setServerName(TrekUtils.getHostname())
                  .setClientName(he.getRemoteAddress().getAddress().getHostAddress())
                  .setRedirectFrom(redirectFrom.orElse(TrekUtils.getHostname()))
                  .build();
          AccessTask task = server.getTaskExecutor().submit(request);
          if (!async) {
            task.waitCompletion();
          }
          AccessStatus status = task.getStatus();
          allDone = true;
          if (needReturnJSON) {
            return new JsonResult(status, server.enableGzip());
          } else {
            return null;
          }
        } finally {
          if (!allDone) {
            TrekUtils.closeWithLog(iChannel, LOG);
            TrekUtils.closeWithLog(oChannel, LOG);
          }
        }
      };
    } else if (path.contains("/data/list")) {
      worker = (WebServer server, HttpExchange he) -> {
        DataInfoQuery dataInfoQuery = DataInfoQuery.parse(parseQuery(he.getRequestURI().getQuery()), server.getMapping());
        DataSystem fromDS = DataSystem.getInstance(dataInfoQuery, server.getConfiguration());
        JsonIO json = new JsonIO(server.getJsonCapacity(), server.enableGzip());
        try (CloseableIterator<DataInfo> iter = fromDS.list(dataInfoQuery);
                JsonWriter writer = json.getWriter()) {
          while (iter.hasNext()) {
            DataInfo.write(writer, iter.next());
          }
        }
        return new JsonResult(HttpStatusCode.OK, json);
      };
    } else if (path.contains("/task/list")) {
      worker = (WebServer server, HttpExchange he) -> {
        JsonIO json = new JsonIO(server.getJsonCapacity(), server.enableGzip());
        AccessStatusQuery query = AccessStatusQuery.parse(parseQuery(he.getRequestURI().getQuery()));
        try (CloseableIterator<AccessStatus> iter = server.getLogger().list(query);
                JsonWriter writer = json.getWriter()) {
          while (iter.hasNext()) {
            AccessStatus.write(writer, iter.next());
          }
        }
        return new JsonResult(HttpStatusCode.OK, json);
      };
    } else {
      worker = (WebServer server, HttpExchange he) -> {
        return new JsonResult(UNKNOWN_ACTION, server.enableGzip());
      };
    }
    return new MethodHandler(worker);
  }

  private static Map<String, String> parseQuery(final String rawQuery) throws UriParseIOException {
    Map<String, String> query = new TreeMap<>();
    if (rawQuery == null || rawQuery.length() == 0) {
      return query;
    }
    for (String q1 : rawQuery.split("&")) {
      int index = q1.indexOf("=");
      if (index == 0
              || index == -1
              || index == (q1.length() - 1)) {
        throw new UriParseIOException("Invalied format of query " + q1);
      }
      String key = q1.substring(0, index);
      String value = q1.substring(index + 1);
      query.put(key.toLowerCase(), TrekUtils.decode(value));
    }
    return query;
  }
}
