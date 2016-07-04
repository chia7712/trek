package com.spright.trek;

import com.spright.trek.datasystem.Protocol;
import org.apache.hadoop.hbase.util.Bytes;

public final class DConstants {

  private DConstants() {
  }
  /**
   * Parameter name for the root dir in ZK for this cluster.
   */
  public static final String ZOOKEEPER_ROOT
          = "trek.zookeeper.root";
  public static final String DEFAULT_ZOOKEEPER_ROOT
          = "/trek";
  public static final String ZOOKEEPER_QUORUM
          = "trek.zookeeper.quorum";
  public static final String ZOOKEEPER_SESSION_TIMEOUT
          = "trek.zookeeper.session.timeout";
  public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT
          = 180 * 1000;
  public static final String ACCESS_LOG_TTL_IN_SECOND
          = "trek.access.log.ttl.in.second";
  public static final int DEFAULT_ACCESS_LOG_TTL_IN_SECOND
          = -1;
  public static final String ENABLE_GZIP_RESPONSE
          = "trek.enable.gzip.response";
  public static final boolean DEFAULT_ENABLE_GZIP_RESPONSE
          = false;
  public static final String ENABLE_SINGLE_MODE
          = "trek.enable.singleton.mode";
  public static final boolean DEFAULT_ENABLE_SINGLETON
          = false;
  public static final int SCANNER_CACHING = 100;
  public static final String EMPTY_STRING = "";
  public static final String FTP_TIME_FORMAT = "YYYYMMDDHHMMSS.sss";
  /**
   * Timestamp format.
   */
  public static final String DEFAULT_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
  public static final String TREK_NAMESPACE = "trek.namespace";
  public static final String DEFAULT_TREK_NAMESPACE
          = "CHIA7712";
  public static final String TREK_LARGE_DATA_THRESHOLD
          = "trek.large.data.threshold";
  public static final int DEFAULT_TREK_LARGE_DATA_THRESHOLD
          = 10 * 1024 * 1024;
  public static final String DATA_BUFFER_INITIAL_SIZE
          = "trek.data.buffer.initial.size";
  public static final int DEFAULT_DATA_BUFFER_INITIAL_SIZE
          = 1 * 1024 * 1024;
  public static final String JSON_BUFFER_LIMIT
          = "trek.json.buffer.limit";
  public static final int DEFAULT_JSON_BUFFER_LIMIT
          = DEFAULT_DATA_BUFFER_INITIAL_SIZE;
  public static final byte[] DEFAULT_FAMILY = Bytes.toBytes("f");
  public static final byte[] HDS_TABLE_FAMILY = DEFAULT_FAMILY;
  public static final byte[] HDS_DATA_UPLOAD_TIME_QUALIFIER = Bytes.toBytes("t");
  public static final byte[] HDS_DATA_SIZE_QUALIFIER = Bytes.toBytes("s");
  public static final byte[] HDS_DATA_CONTENT_QUALIFIER = Bytes.toBytes("v");
  public static final byte[] HDS_DATA_LINK_QUALIFIER = Bytes.toBytes("k");

  public static final String LOCK_NAME = "LOCK";

  public static final byte[] LOCK_TABLE_FAMILY = DEFAULT_FAMILY;
  public static final byte[] LOCK_CURRENT_TIME_QUALIFIER = Bytes.toBytes("l");

  public static final String MAPPING_NAME = "MAPPING";
  public static final byte[] MAPPING_TABLE_FAMILY = DEFAULT_FAMILY;
  public static final byte[] MAPPING_DOMAIN_QUALIFIER = Bytes.toBytes("domain");
  public static final byte[] MAPPING_USER_QUALIFIER = Bytes.toBytes("user");
  public static final byte[] MAPPING_PASSWORD_QUALIFIER = Bytes.toBytes("pwd");
  public static final byte[] MAPPING_HOST_QUALIFIER = Bytes.toBytes("host");
  public static final byte[] MAPPING_PORT_QUALIFIER = Bytes.toBytes("port");

  public static final String TASK_LOG_NAME = "TASKLOGGER";
  public static final byte[] TASK_FAMILY = DEFAULT_FAMILY;
  public static final byte[] TASK_REDIRECT_QUALIFIER = Bytes.toBytes("r");
  public static final byte[] TASK_SERVER_QUALIFIER = Bytes.toBytes("s");
  public static final byte[] TASK_CLIENT_QUALIFIER = Bytes.toBytes("c");
  public static final byte[] TASK_FROM_QUALIFIER = Bytes.toBytes("f");
  public static final byte[] TASK_TO_QUALIFIER = Bytes.toBytes("t");
  public static final byte[] TASK_STATE_QUALIFIER = Bytes.toBytes("ss");
  public static final byte[] TASK_PROGRESS_QUALIFIER = Bytes.toBytes("p");
  public static final byte[] TASK_START_TIME_QUALIFIER = Bytes.toBytes("st");
  public static final byte[] TASK_ELAPSED_QUALIFIER = Bytes.toBytes("e");
  public static final byte[] TASK_EXPECTED_SIZE_QUALIFIER = Bytes.toBytes("es");
  public static final byte[] TASK_TRANSFERRED_SIZE_QUALIFIER = Bytes.toBytes("ts");

  public static final String URI_MAPPING_ID = "id";
  public static final String URI_MAPPING_DOMAIN = "domain";
  public static final String URI_MAPPING_USER = "user";
  public static final String URI_MAPPING_PASSWORD = "password";
  public static final String URI_MAPPING_HOST = "host";
  public static final String URI_MAPPING_PORT = "port";
  public static final String URI_MAPPING_MIN_PORT = "minport";
  public static final String URI_MAPPING_MAX_PORT = "maxport";
  public static final String HDS_ROOT_PATH
          = "trek.hds.root.path";
  public static final String DEFAULT_HDS_ROOT_PATH
          = "/trek";
  public static final String DATA_TMP_EXTENSION = ".part";
  public static final String ACCESS_HANDLER_NUMBER
          = "trek.access.handler.number";
  public static final int DEFAULT_ACCESS_HANDLER_NUMBER
          = 10;
  public static final String RESTFUL_SERVER_CONNECTION_NUMBER
          = "trek.restful.server.connection.number";
  public static final int DEFAULT_RESTFUL_SERVER_CONNECTION_NUMBER
          = DEFAULT_ACCESS_HANDLER_NUMBER;
  public static final String LOCK_DISABLE
          = "trek.lock.disable";
  public static final boolean DEFAULT_LOCK_DISABLE
          = false;
  public static final String OPERATION_BATCH_LIMIT
          = "trek.operation.batch.limit";
  public static final int DEFAULT_OPERATION_BATCH_LIMIT
          = 30;
  public static final boolean DEFAULT_URI_DATA_KEEP
          = true;
  public static final Protocol DEFAULT_URI_DATA_SAVED_LOCATION
          = Protocol.HBASE;
  public static final String URI_DATA_REDIRECT_FROM
          = "redirectfrom";
  public static final String URI_DATA_ASYNC
          = "async";
  public static final boolean DEFAULT_URI_DATA_ASYNC
          = false;
  public static final String URI_DATA_FROM
          = "from";
  public static final String URI_DATA_ENABLE_WILDCARD
          = "enablewildcard";
  public static final String URI_DATA_TO
          = "to";
  public static final String URI_DATA_KEEP
          = "keep";
  public static final String URI_DATA_NEED_FILE
          = "file";
  public static final boolean DEFAULT_URI_DATA_NEED_FILE
          = true;
  public static final String URI_DATA_NEED_DIRECTORY
          = "directory";
  public static final boolean DEFAULT_URI_DATA_NEED_DIRECTORY
          = false;
  public static final String URI_DATA_UPLOAD_TIME
          = "time";
  public static final String URI_DATA_SIZE
          = "size";
  public static final String URI_DATA_LIMIT
          = "limit";
  public static final int DEFAULT_URI_DATA_LIMIT
          = 100;
  public static final String URI_DATA_OFFSET
          = "offset";
  public static final int DEFAULT_URI_DATA_OFFSET
          = 0;
  public static final String URI_DATA_ORDER_ASC
          = "asc";
  public static final boolean DEFAULT_URI_DATA_ORDER_ASC
          = true;
  public static final String URI_DATA_ORDER_BY
          = "orderby";
  public static final String URI_TASK_REDIRECT
          = URI_DATA_REDIRECT_FROM;
  public static final String URI_TASK_SERVER_NAME
          = "servername";
  public static final String URI_TASK_CLIENT_NAME
          = "clientname";
  public static final String URI_TASK_ID
          = "id";
  public static final String URI_TASK_FROM
          = URI_DATA_FROM;
  public static final String URI_TASK_TO
          = URI_DATA_TO;
  public static final String URI_TASK_STATE
          = "state";
  public static final String URI_TASK_PROGRESS
          = "progress";
  public static final String URI_TASK_START_TIME
          = "starttime";
  public static final String URI_TASK_ELAPSED
          = "elapsed";
  public static final String URI_TASK_EXPECTED_SIZE
          = "expectedsize";
  public static final String URI_TASK_TRANSFERRED_SIZE
          = "transferredsize";

  public static final String RESTFUL_SERVER_BINDING_ARRRESS
          = "trek.restful.server.binding.address";
  public static final String DEFAULT_RESTFUL_SERVER_BINDING_ARRRESS
          = "0.0.0.0";
  public static final String RESTFUL_SERVER_BINDING_PORT
          = "trek.restful.server.binding.port";
  public static final short DEFAULT_RESTFUL_SERVER_BINDING_PORT
          = 7777;
}
