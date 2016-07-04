package com.spright.trek.io;

import com.spright.trek.utils.TrekUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Outputs data to memory if there are enough space. Otherwise, the output will
 * introduced to a infinite output created by {@link IOFactory}. The example is
 * as following: 1) get output stream 2) close output stream 3) get input stream
 * 4) close input stream The input stream is valid only if the output stream is
 * clsoed correctly Don't REUSE!!
 */
public final class InfiniteAccesser {

  /**
   * Log.
   */
  private static final Log LOG = LogFactory.getLog(InfiniteAccesser.class);
  /**
   * In-memory buffer.
   */
  private final ByteArrayOutput byteOutput;
  /**
   * Supplies the infinite output.
   */
  private final IOFactory factory;
  /**
   * Infinite output.
   */
  private OutputStream infiniteOutput;
  /**
   * In-memory or not.
   */
  private volatile boolean inMemory = true;
  /**
   * Output is closed or not.
   */
  private volatile boolean outputIsClosed = false;
  /**
   * The bytes has writed.
   */
  private volatile long hasWrite = 0;
  /**
   * The output stream to return.
   */
  private OutputStream rvalOutput = null;
  /**
   * The input stream to return.
   */
  private InputStream rvalInput = null;

  /**
   * Use the infinite output directly.
   *
   * @param factory To create infinite output
   * @throws java.io.IOException Filed to create output stream by
   * {@link IOFactory}
   */
  public InfiniteAccesser(final IOFactory factory) throws IOException {
    this.factory = factory;
    this.inMemory = false;
    this.infiniteOutput = factory.createOutputStream();
    this.byteOutput = null;
  }

  public InfiniteAccesser(final int capacity) {
    checkMemorySetting(Integer.MAX_VALUE, capacity);
    this.factory = null;
    this.inMemory = true;
    this.byteOutput = new ByteArrayOutput(Integer.MAX_VALUE, capacity);
    this.infiniteOutput = null;
  }

  /**
   * @param limit limit
   * @param capacity Iniitial capacity
   * @param factory To create infinite output
   * @throws java.io.IOException Filed to create output stream by
   * {@link IOFactory}
   */
  public InfiniteAccesser(final int limit, final int capacity,
          final IOFactory factory) throws IOException {
    checkMemorySetting(limit, capacity);
    this.factory = factory;
    this.inMemory = true;
    this.byteOutput = new ByteArrayOutput(limit, capacity);
    this.infiniteOutput = null;
  }

  /**
   * Push up the error happen.
   *
   * @param limit Limit
   * @param capacity Initial capacity
   */
  public static void checkMemorySetting(final int limit,
          final int capacity) {
    if (limit <= 0 || capacity <= 0 || limit < capacity) {
      String msg = "The limit must be greate than initial capacity"
              + ", threshold : " + limit
              + ", initial capacity : " + capacity;
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
  }

  /**
   * @return The bytes to write
   */
  public long getSize() {
    return hasWrite;
  }

  /**
   * @param incrementBytes The bytes will be writed
   * @return Vaild output
   * @throws IOException If failed to switch in-memory buffer to infinite
   * output.
   */
  private OutputStream getValidOutputStream(final int incrementBytes)
          throws IOException {
    if (!inMemory) {
      return infiniteOutput;
    } else if (byteOutput.ensureCapacity(incrementBytes)) {
      return byteOutput;
    } else {
      infiniteOutput = checkIOFactory().createOutputStream();
      infiniteOutput.write(byteOutput.getBuffer(),
              byteOutput.getOffset(), byteOutput.getLength());
      byteOutput.close();
      inMemory = false;
      return infiniteOutput;
    }
  }

  /**
   * @throws IOException If the output is closed.
   */
  private void checkOutputStatus() throws IOException {
    if (outputIsClosed) {
      String msg = "The infinite output is closed";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  /**
   * @return True if the output is closed
   */
  public boolean isOutputClosed() {
    return outputIsClosed;
  }

  /**
   * @throws IOException If the output has not be closed
   */
  private void checkOutputClosed() throws IOException {
    if (!outputIsClosed) {
      String msg = "The infinite output not be closed";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  private IOFactory checkIOFactory() {
    if (factory == null) {
      String msg = "The io factory is null,"
              + " we cannot flush data to the persistent storage";
      LOG.error(msg);
      throw new RuntimeException(msg);
    }
    return factory;
  }

  /**
   * Throws exception if the output has not be closed.
   *
   * @return The input stream with the previous writed data
   * @throws IOException If failed to generate the input stream
   */
  public InputStream getInputStream() throws IOException {
    checkOutputClosed();
    if (rvalInput != null) {
      return rvalInput;
    }
    if (inMemory) {
      rvalInput = new ByteArrayInputStream(byteOutput.getBuffer(),
              byteOutput.getOffset(), byteOutput.getLength());
    } else {
      rvalInput = checkIOFactory().createInputStream();
    }
    return rvalInput;
  }

  /**
   * @return True if the data is all in memory.
   */
  public boolean isInMemory() {
    return inMemory;
  }

  /**
   * @return The input buffer
   * @throws IOException If the data in not in-memory
   */
  public byte[] getBuffer() throws IOException {
    checkInMemory();
    return byteOutput.getBuffer();
  }

  /**
   * @return The offset in the buffer of the first byte to read
   * @throws IOException If the data in not in-memory
   */
  public int getBufferOffset() throws IOException {
    checkInMemory();
    return byteOutput.getOffset();
  }

  /**
   * @return The maximum number of bytes to read from the buffer
   * @throws IOException If the data in not in-memory
   */
  public int getBufferLength() throws IOException {
    checkInMemory();
    return byteOutput.getLength();
  }

  /**
   * Throws the exception if the data in not in-memory.
   *
   * @throws IOException if the data in not in-memory
   */
  private void checkInMemory() throws IOException {
    if (!inMemory) {
      String msg = "The data is not in-memory";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }

  /**
   * Creates a output stream first time.
   *
   * @return A outptu stream which can auto transfer data to infinte output if
   * the data reach the threshold
   * @throws IOException If failed to switch output
   */
  public OutputStream getOutputStream() throws IOException {
    checkOutputStatus();
    if (rvalOutput != null) {
      return rvalOutput;
    }
    rvalOutput = new OutputStream() {
      @Override
      public void write(final int b) throws IOException {
        checkOutputStatus();
        getValidOutputStream(1).write(b);
        ++hasWrite;
      }

      @Override
      public void flush() throws IOException {
        checkOutputStatus();
        if (inMemory) {
          byteOutput.flush();
        } else {
          infiniteOutput.flush();
        }
      }

      @Override
      public void write(final byte[] b, final int off, final int len)
              throws IOException {
        checkOutputStatus();
        getValidOutputStream(len).write(b, off, len);
        hasWrite += len;
      }

      @Override
      public void close() throws IOException {
        if (inMemory) {
          byteOutput.close();
        } else {
          infiniteOutput.close();
        }
        outputIsClosed = true;
      }
    };
    return rvalOutput;
  }

  /**
   * HDFS-based IOFactory. The infinite output is implemeneted by
   * {@link FSDataOutputStream}.
   */
  public static final class HdfsIOFactory implements IOFactory {

    /**
     * Hadoop file system.
     */
    private final FileSystem fs;
    /**
     * The tmp path.
     */
    private final Path path;

    /**
     * @param f Hadoop file system.
     * @param p The tmp path.
     */
    public HdfsIOFactory(final FileSystem f, final Path p) {
      fs = f;
      path = p;
    }

    /**
     * @return The path to buffer data
     */
    public Path getPath() {
      return path;
    }

    @Override
    public OutputStream createOutputStream() throws IOException {
      if (fs.exists(path)) {
        throw new IOException("The " + path.toString()
                + " is existed");
      }
      return fs.create(path);
    }

    @Override
    public InputStream createInputStream()
            throws IOException {
      return fs.open(path);
    }
  }

  /**
   * File-based IOFactory. The infinite output is implemeneted by
   * {@link FileOutputStream}.
   */
  public static final class FileIOFactory implements IOFactory {

    /**
     * Buffer file.
     */
    private final File file;
    /**
     * Deletes the tmp file after closing input stream.
     */
    private final boolean autoDelete;

    /**
     * Constructs a FileIOFactory with random tmp file.
     *
     * @throws IOException If a file could not be created
     */
    public FileIOFactory() throws IOException {
      file = File.createTempFile(this.getClass().getName(),
              String.valueOf(System.currentTimeMillis()));
      autoDelete = true;
    }

    /**
     * @param f Tmp file to save data
     */
    public FileIOFactory(final File f) {
      file = f;
      autoDelete = false;
    }

    /**
     * @return The buffer file
     */
    public File getFile() {
      return file;
    }

    @Override
    public OutputStream createOutputStream() throws IOException {
      return new FileOutputStream(file);
    }

    @Override
    public InputStream createInputStream()
            throws IOException {
      final InputStream innerInput = new FileInputStream(file);
      return new InputStream() {
        @Override
        public int read() throws IOException {
          return innerInput.read();
        }

        @Override
        public int read(final byte[] b, final int off,
                final int len) throws IOException {
          return innerInput.read(b, off, len);
        }

        @Override
        public long skip(final long n) throws IOException {
          return innerInput.skip(n);
        }

        @Override
        public int available() throws IOException {
          return innerInput.available();
        }

        @Override
        public void close() throws IOException {
          innerInput.close();
          if (autoDelete) {
            TrekUtils.closeWithLog(() -> file.delete(), null);
          }
        }

        @Override
        public void mark(final int readlimit) {
          innerInput.mark(readlimit);
        }

        @Override
        public void reset() throws IOException {
          innerInput.reset();
        }

        @Override
        public boolean markSupported() {
          return innerInput.markSupported();
        }
      };
    }
  }

  /**
   * Generates the infinite output and the input from the closed infinite
   * output.
   */
  public interface IOFactory {

    /**
     * @return A infinite capacity.
     * @throws IOException If failed to create output
     */
    OutputStream createOutputStream() throws IOException;

    /**
     * @return A input to read the previous output
     * @throws IOException If failed to read output
     */
    InputStream createInputStream() throws IOException;
  }

  /**
   * Expose the private members which consists of buffer and count to public. It
   * is not thread-safe, so DON'T fucking use this object in multi-thread.
   */
  public static final class ByteArrayOutput extends ByteArrayOutputStream {

    /**
     * The memory size limit.
     */
    private final int limit;

    /**
     * Creates a new byte array output stream, with a buffer capacity of the
     * specified size, in bytes.
     *
     * @param limit The max size
     * @param capacity the initial size.
     * @exception IllegalArgumentException if size is negative.
     */
    public ByteArrayOutput(final int limit, final int capacity) {
      super(capacity);
      this.limit = limit;
    }

    /**
     * Ensures that it can hold at least the number of elements specified by the
     * minimum capacity argument.
     *
     * @param incremetBytes the desired bytes to add
     * @return True if there are enough capacity
     */
    boolean ensureCapacity(final int incremetBytes) {
      return limit - (count + incremetBytes) >= 0;
    }

    /**
     * @return Internal buffer
     */
    public byte[] getBuffer() {
      return buf;
    }

    /**
     * @return buffer length
     */
    public int getLength() {
      return count;
    }

    /**
     * @return buffer offset
     */
    public int getOffset() {
      return 0;
    }
  }
}
