package com.spright.trek.io;

import java.io.IOException;
import java.io.OutputStream;

public class OutputStreamAdapter extends OutputStream {

  private final OutputStream output;

  public OutputStreamAdapter(final OutputStream output) {
    this.output = output;
  }

  @Override
  public void write(int b) throws IOException {
    output.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    output.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    output.flush();
  }

  @Override
  public void close() throws IOException {
    output.close();
  }
}
