package com.spright.trek.datasystem;

import com.spright.trek.datasystem.request.DataType;
import com.spright.trek.datasystem.request.DataOwner;
import com.spright.trek.datasystem.request.DataInfo;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import com.spright.trek.datasystem.request.DataInfoQuery;
import com.spright.trek.datasystem.request.ReadDataRequest;
import com.spright.trek.datasystem.request.WriteDataRequest;
import com.spright.trek.exception.HttpIOException;
import com.spright.trek.mapping.AccountInfo;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.utils.TrekUtils;

public class HttpDataSystem extends DataSystem {

  private static final Log LOG = LogFactory.getLog(HttpDataSystem.class);
  private static final Base64 ENCODER = new Base64();
  private static final String BOUNDARY = "--------";
  private static final String LINE_END = "\r\n";

  public HttpDataSystem(final Configuration config) throws Exception {
    super(config, Protocol.HTTP);
  }

  private static HttpURLConnection creatHttpURLConnection(
          final AccountInfo info, final String catalog) throws IOException {
    URL url = new URL("http://"
            + info.toString(false)
            + catalog);
    URLConnection urlConnection = url.openConnection();
    if (url.getUserInfo() != null) {
      String basicAuth = "Basic " + new String(ENCODER.encode(url.getUserInfo().getBytes()), "UTF-8");
      //Long passwords cause new line character
      basicAuth = basicAuth.replaceAll("\n", "");
      urlConnection.setRequestProperty("Authorization", basicAuth);
    }
    if (urlConnection instanceof HttpURLConnection) {
      HttpURLConnection httpCon = (HttpURLConnection) urlConnection;
      return httpCon;
    }
    throw new HttpIOException("Failed to convert URLConnection to HttpURLConnection");
  }

  @Override
  protected InputChannel internalOpen(final ReadDataRequest request) throws IOException {
    String host = DataSystem.getHostOrThrow(request);
    HttpURLConnection httpConn = creatHttpURLConnection(
            request.getAccountInfo(), request.getPath().getCatalog());
    httpConn.setDoInput(true);
    httpConn.setRequestMethod("GET");
    httpConn.setInstanceFollowRedirects(true);
    httpConn.connect();
    int responseCode = httpConn.getResponseCode();
    if (responseCode != HttpURLConnection.HTTP_OK) {
      throw new IOException("Failed to connection http server with erro"
              + " code " + responseCode);
    }
    String fileName = request.getPath().getName();
    String disposition = httpConn.getHeaderField("Content-Disposition");
    final long ts = httpConn.getLastModified();
    final int contentLength = httpConn.getContentLength();
    if (disposition != null) {
      for (String s : disposition.split(";")) {
        int index = s.indexOf("=");
        if (index != -1) {
          fileName = s.substring(index + 1).replaceAll("\"", "");
          break;
        }
      }
    }
    if (fileName == null || fileName.length() == 0) {
      throw new IOException("Failed to find the file name");
    }
    final String name = fileName;
    DataInfo info = DataInfo.newBuilder()
            .setRequest(request)
            .setType(DataType.FILE)
            .setOwner(DataOwner.newSingleOwner(host))
            .setSize(contentLength)
            .setUploadTime(ts)
            .build();
    return new InputChannel() {
      private final InputStream input = httpConn.getInputStream();

      @Override
      public void close() throws IOException {
        TrekUtils.closeWithLog(input, LOG);
        httpConn.disconnect();
      }

      @Override
      public DataInfo getInfo() {
        return info;
      }

      @Override
      public InputStream getInputStream() {
        return input;
      }
    };

  }

  @Override
  protected OutputChannel internalCreate(final WriteDataRequest request) throws IOException {
    DataSystem.getHostOrThrow(request);
    final String name = request.getPath().getName();
    final long expectedSize = DataSystem.getInputSizeOrThrow(request);
    HttpURLConnection httpConn = creatHttpURLConnection(
            request.getAccountInfo(), request.getPath().getCatalog());
    httpConn.setDoOutput(true);
    httpConn.setInstanceFollowRedirects(true);
    httpConn.setRequestMethod("POST");
    httpConn.setRequestProperty("User-Agent", "CodeJava Agent");
    httpConn.setRequestProperty("Content-Type",
            "multipart/form-data; boundary=" + BOUNDARY);
    httpConn.connect();
    final DataOutputStream output = new DataOutputStream(httpConn.getOutputStream());
    try {
      output.writeBytes("--");
      output.writeBytes(BOUNDARY);
      output.writeBytes(LINE_END);
      output.writeBytes("Content-Disposition: form-data; filename=\""
              + name + "\";");
      output.writeBytes(LINE_END);
      output.writeBytes("Content-Type: application/octet-stream;");
      output.writeBytes(LINE_END);
      output.writeBytes("Content-length: " + String.valueOf(expectedSize));
      output.writeBytes(LINE_END);
      output.writeBytes(LINE_END);
    } catch (IOException e) {
      TrekUtils.closeWithLog(output, LOG);
      httpConn.disconnect();
      throw e;
    }
    return new OutputChannel() {
      @Override
      public WriteDataRequest getRequest() {
        return request;
      }

      @Override
      public void close() throws IOException {
        try {
          output.writeBytes(LINE_END);
          output.writeBytes("--");
          output.writeBytes(BOUNDARY);
          output.writeBytes("--");
          output.writeBytes(LINE_END);
          output.close();
          int status = httpConn.getResponseCode();
          if (status != HttpURLConnection.HTTP_OK) {
            throw new IOException("Failed to upload data to"
                    + " http server with error code " + status);
          }
        } finally {
          httpConn.disconnect();
        }
      }

      @Override
      public OutputStream getOutputStream() {
        return output;
      }

      @Override
      public void recover() throws IOException {
      }
    };
  }

  @Override
  protected void close() throws Exception {
  }

  @Override
  protected void internalDelete(DataInfo info) throws IOException {
    throw new IOException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  protected CloseableIterator<DataInfo> internalList(DataInfoQuery request) throws IOException {
    throw new IOException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
}
