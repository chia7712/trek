package com.spright.trek.datasystem;

import com.spright.trek.datasystem.request.DataInfo;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.spright.trek.web.HttpStatusCode;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import com.spright.trek.datasystem.request.DataInfoQuery;
import com.spright.trek.datasystem.request.ReadDataRequest;
import com.spright.trek.datasystem.request.WriteDataRequest;
import com.spright.trek.exception.HttpIOException;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.utils.TrekUtils;
import com.spright.trek.utils.HttpUtils;
import com.spright.trek.web.HttpUploadParser;

public class LocalDataSystem extends DataSystem {

  public LocalDataSystem(final Configuration config) throws Exception {
    super(config, Protocol.LOCAL);
  }

  @Override
  protected InputChannel internalOpen(ReadDataRequest request) throws IOException {
    HttpExchange he = request.getAttach()
            .filter(o -> o instanceof HttpExchange)
            .map(o -> (HttpExchange) o)
            .orElseThrow(() -> new HttpIOException(
                    "Failed to get com.sun.net.httpserver.HttpExchange from the attach"));
    String boundary = HttpUtils.parseForBoundary(he.getRequestHeaders())
            .orElseThrow(() -> new HttpIOException("Boundary Required", HttpStatusCode.BAD_REQUEST));
    HttpUploadParser parser = HttpUploadParser.instance(request,
            TrekUtils.getHostname(), he.getRequestBody(), boundary);
    return parser.seekNext().orElseThrow(
            () -> new HttpIOException("Failed to parse the upload body"));
  }

  @Override
  protected OutputChannel internalCreate(WriteDataRequest request) throws IOException {
    HttpExchange he = request.getAttach()
            .filter(o -> o instanceof HttpExchange)
            .map(o -> (HttpExchange) o)
            .orElseThrow(() -> new HttpIOException(
                    "Failed to get com.sun.net.httpserver.HttpExchange from the attach"));
    final long size = DataSystem.getInputSizeOrThrow(request);
    final String name = request.getPath().getName();
    Headers headers = he.getResponseHeaders();
    headers.set("Content-Type", "application/octet-stream");
    headers.set("content-disposition", "filename=\"" + name + "\"");
    he.sendResponseHeaders(HttpStatusCode.OK.get(), size);
    OutputStream output = he.getResponseBody();
    return new OutputChannel() {
      @Override
      public WriteDataRequest getRequest() {
        return request;
      }

      @Override
      public OutputStream getOutputStream() {
        return output;
      }

      @Override
      public void recover() throws IOException {
      }

      @Override
      public void close() throws IOException {
        he.close();
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
