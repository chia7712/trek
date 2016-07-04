package com.spright.trek.io.json;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.spright.trek.io.InfiniteAccesser;
import com.spright.trek.io.InfiniteAccesser.FileIOFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class JsonIO {

  private volatile JsonReader reader;
  private volatile JsonWriter writer;
  private final InfiniteAccesser access;
  private final boolean gzip;

  public JsonIO(final int limit, final int capacity, final boolean gzip) throws IOException {
    access = new InfiniteAccesser(limit, capacity, new FileIOFactory());
    this.gzip = gzip;
    if (gzip) {
      writer = new JsonWriter(new OutputStreamWriter(new GZIPOutputStream(access.getOutputStream())));
    } else {
      writer = new JsonWriter(new OutputStreamWriter(access.getOutputStream()));
    }
  }

  public JsonIO(final int capacity, final boolean gzip) throws IOException {
    this(capacity, capacity, gzip);
  }

  public long getSize() {
    return access.getSize();
  }

  public boolean isGzip() {
    return gzip;
  }

  public InputStream getRawInputStream() throws IOException {
    return access.getInputStream();
  }

  public JsonReader getReader() throws IOException {
    if (!access.isOutputClosed()) {
      throw new IOException("The output has not be closed");
    }
    if (reader == null) {
      if (gzip) {
        reader = new JsonReader(new InputStreamReader(new GZIPInputStream(access.getInputStream())));
      } else {
        reader = new JsonReader(new InputStreamReader(access.getInputStream()));
      }
    }
    return reader;
  }

  public JsonWriter getWriter() throws IOException {
    if (writer == null) {
      throw new IOException("The writer is closed");
    }
    return writer;
  }
}
