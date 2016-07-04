package com.spright.trek.web;

import com.google.gson.stream.JsonWriter;
import com.spright.trek.exception.IOExceptionWithErrorCode;
import com.spright.trek.io.json.JsonIO;
import com.spright.trek.task.AccessStatus;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.lang.exception.ExceptionUtils;
import static org.apache.hadoop.fs.FileSystem.LOG;

public final class MethodHandler {

  @FunctionalInterface
  public interface Worker {

    JsonResult internalHandle(final WebServer server, final HttpExchange he) throws Exception;
  }
  private static final HttpStatusCode DEFAULT_CODE = HttpStatusCode.INTERNAL_SERVER_ERROR;
  private final Worker worker;

  public MethodHandler(final Worker worker) {
    this.worker = worker;
  }

  public final void handle(final WebServer server, final HttpExchange he) throws IOException {
    JsonResult result;
    try {
      result = worker.internalHandle(server, he);
    } catch (Exception e) {
      LOG.error("Error on uri:" + he.getRequestURI(), e);
      result = handleError(server, e);
    }
    Headers headers = he.getResponseHeaders();
    headers.set("Content-Type", "application/json;charset=utf-8");
    headers.set("content-disposition", "filename=\""
            + (System.currentTimeMillis() + ".json") + "\"");
    if (result.getJson().isGzip()) {
      headers.set("Accept-Encoding", "gzip");
    }
    he.sendResponseHeaders(result.getCode().get(), result.getJson().getSize());
    OutputStream output = he.getResponseBody();
    try (InputStream input = result.getJson().getRawInputStream()) {
      byte[] buf = new byte[1024];
      int rval;
      while ((rval = input.read(buf)) != -1) {
        output.write(buf, 0, rval);
      }
    }
    he.close();
  }

  private static JsonResult handleError(final WebServer server, final Exception e) throws IOException {
    return new JsonResult(e, server.enableGzip());
  }

  public static class JsonResult {

    private final HttpStatusCode code;
    private final JsonIO json;

    JsonResult(final AccessStatus info, final boolean gzip) throws IOException {
      json = new JsonIO(Integer.MAX_VALUE, 1024, gzip);
      try (JsonWriter writer = json.getWriter()) {
        AccessStatus.write(writer, info);
      }
      code = HttpStatusCode.OK;
    }

    JsonResult(final Exception e, final boolean gzip) throws IOException {
      json = new JsonIO(Integer.MAX_VALUE, 1024, gzip);
      try (JsonWriter writer = json.getWriter()) {
        writer.beginObject()
                .name("exception").value(e.getClass().getName())
                .name("msg").value(e.getMessage())
                .name("stack").value(ExceptionUtils.getStackTrace(e))
                .endObject();
      }
      code = (e instanceof IOExceptionWithErrorCode)
              ? ((IOExceptionWithErrorCode) e).getHttpStatus()
              : DEFAULT_CODE;
    }

    JsonResult(final HttpStatusCode code,
            final JsonIO json) {
      this.code = code;
      this.json = json;
    }

    HttpStatusCode getCode() {
      return code;
    }

    JsonIO getJson() {
      return json;
    }
  }
}
