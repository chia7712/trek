package com.spright.trek.io;

import java.io.IOException;
import java.io.InputStream;

public class InputStreamAdapter extends InputStream {

  private final InputStream input;

  public InputStreamAdapter(final InputStream input) {
    this.input = input;
  }

  @Override
  public int read() throws IOException {
    return input.read();
  }

  @Override
  public int read(byte b[]) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return input.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return input.skip(n);
  }

  @Override
  public int available() throws IOException {
    return input.available();
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  @Override
  public void mark(int readlimit) {
    input.mark(readlimit);
  }

  @Override
  public void reset() throws IOException {
    input.reset();
  }

  @Override
  public boolean markSupported() {
    return input.markSupported();
  }
}
