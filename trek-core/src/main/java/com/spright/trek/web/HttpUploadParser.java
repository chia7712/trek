package com.spright.trek.web;

import com.spright.trek.datasystem.request.DataInfo;
import com.spright.trek.datasystem.request.DataOwner;
import com.spright.trek.datasystem.request.DataType;
import com.spright.trek.datasystem.request.ReadDataRequest;
import com.spright.trek.exception.HttpIOException;
import com.spright.trek.io.AtomicCloseable;
import com.spright.trek.utils.HttpUtils;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.BufferedInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Parses the Form-based File Upload in HTML.
 *
 * @see http://tools.ietf.org/html/rfc1867
 */
public abstract class HttpUploadParser extends AtomicCloseable {

  private static final Log LOG = LogFactory.getLog(HttpUploadParser.class);

  public static HttpUploadParser instance(final ReadDataRequest request,
          final String host, final InputStream input, final String boundary) {
    return new HttpUploadParserImpl(request, host, input, boundary);
  }

  public abstract Optional<HttpUploadReader> seekNext() throws IOException;

  private static class HttpUploadParserImpl extends HttpUploadParser {

    private static final int MAX_HEADER_SIZE = 2 * 1024;
    /**
     * HTTP /r.
     */
    private static final int BACK_VALUE = (int) '\r';
    /**
     * HTTP /n.
     */
    private static final int NEW_LINE_VALUE = (int) '\n';
    /**
     * Boundary prefix.
     */
    private static final String BOUNDARY_LINE = "--";
    /**
     * Header first key.
     */
    private static final String HEADER_FIRST_KEY = ":";
    /**
     * Header secondary key.
     */
    private static final String HEADER_SECOND_KEY = ";";
    /**
     * HTTP input stream with prefatch function.
     */
    private final PrefetchableInput input;
    /**
     * HTTP boundary.
     */
    private final String boundary;
    /**
     * HTTP POST end boundary.
     */
    private final String endBoundary;
    private final List<DataOwner> hosts;
    private final ReadDataRequest request;
    private boolean isEnd = false;
    private boolean inDataField = false;

    HttpUploadParserImpl(final ReadDataRequest request, final String host,
            final InputStream input, final String boundary) {
      if (boundary == null || boundary.length() == 0) {
        throw new RuntimeException("The boundary is empty");
      }
      this.boundary = BOUNDARY_LINE + boundary;
      this.endBoundary = boundary + BOUNDARY_LINE;
      this.input = new PrefetchableInput(input);
      this.hosts = Arrays.asList(new DataOwner(host, 1.0f));
      this.request = request;
    }

    /**
     * Read a line by the end of "\r\n".
     *
     * @return Line
     * @throws IOException If an I/O error occurs
     */
    String readLine() throws IOException {
      StringBuilder stringBuf = new StringBuilder();
      int endCharCount = 0;
      while (true) {
        int c = input.read();
        switch (endCharCount) {
          case 1:
            if (c == NEW_LINE_VALUE) {
              return stringBuf.toString();
            } else {
              endCharCount = 0;
              stringBuf.append((char) c);
            }
            break;
          default:
            if (c == BACK_VALUE) {
              endCharCount = 1;
            } else {
              endCharCount = 0;
              stringBuf.append((char) c);
            }
            break;
        }
        if (stringBuf.length() >= MAX_HEADER_SIZE) {
          LOG.error("Too large line in header:" + stringBuf.toString());
          throw new HttpPostException("Too large line in header : "
                  + stringBuf.length());
        }
      }
    }

    @Override
    public Optional<HttpUploadReader> seekNext() throws IOException {
      if (isEnd) {
        return Optional.empty();
      }
      if (inDataField) {
        throw new HttpPostException("You cannot seek next HttpUploadReader before"
                + " reading to end of previous HttpUploadReader");
      }
      Map<String, List<String>> fieldHead = new HashMap<>();
      while (true) {
        String line = readLine();
        if (line.length() == 0 && !inDataField) {
          continue;
        }
        // first line = /r/n
        if (line.length() == 0 && inDataField) {
          final long contentLength = HttpUtils.parseForContentLength(fieldHead)
                  .orElseThrow(() -> new HttpIOException("No found of content-length"));
          final String filename = HttpUtils.parseForFileName(fieldHead)
                  .orElseThrow(() -> new HttpIOException("No found of filename"));
          final DataInfo info = DataInfo.newBuilder()
                  .setRequest(request)
                  .setSize(contentLength)
                  .setOwners(hosts)
                  .setType(DataType.FILE)
                  .setUploadTime(System.currentTimeMillis())
                  .build();
          HttpUploadReader rval = new HttpUploadReader() {
            private final long ts = System.currentTimeMillis();

            @Override
            public Map<String, List<String>> getHeader() {
              return fieldHead;
            }

            @Override
            public DataInfo getInfo() {
              return info;
            }

            @Override
            public InputStream getInputStream() {
              return new InputStream() {
                @Override
                public int read() throws IOException {
                  if (!inDataField || isEnd) {
                    return -1;
                  }
                  int endCharCount = 0;
                  while (true) {
                    int c = input.read();
                    if (c == -1) {
                      throw new HttpPostException("The http field data cannot be -1");
                    }
                    switch (endCharCount) {
                      case 1:
                        if (c == NEW_LINE_VALUE) {
                          if (input.equalString(endBoundary)) {
                            isEnd = true;
                            return -1;
                          } else if (input.equalString(boundary)) {
                            inDataField = false;
                            return -1;
                          } else {
                            input.insertDataToPrefetch(c);
                            return BACK_VALUE;
                          }
                        } else {
                          input.insertDataToPrefetch(c);
                          return BACK_VALUE;
                        }
                      default:
                        if (c == BACK_VALUE) {
                          endCharCount = 1;
                        } else {
                          return c;
                        }
                        break;
                    }
                  }
                }
              };
            }

            @Override
            public void close() throws IOException {
            }
          };
          return Optional.of(rval);
        }
        // secondary line = boundary
        if (line.equalsIgnoreCase(boundary)) {
          inDataField = true;
          continue;
        }
        fieldHead.putAll(parseHeaderLine(line));
      }
    }

    /**
     * Parse the field header.
     *
     * @param line The line to parse
     * @return The field header
     */
    private static Map<String, List<String>> parseHeaderLine(final String line) {
      Map<String, List<String>> rval = new HashMap<>();
      final int firstIndex = line.indexOf(HEADER_FIRST_KEY);
      if (firstIndex != -1 && firstIndex < line.length()) {
        String field = line.substring(0, firstIndex);
        String args = line.substring(firstIndex + 1);
        if (args.length() != 0) {
          rval.put(field, Arrays.asList(args.split(HEADER_SECOND_KEY)));
        }
      }
      return rval;
    }

    @Override
    public void internalClose() throws IOException {
      input.close();
    }
  }

  static class PrefetchableInput implements Closeable {

    private final LinkedList<Integer> prefetchData = new LinkedList<>();
    private final InputStream input;
    private byte[] dataBytes;

    PrefetchableInput(final InputStream i) {
      input = new BufferedInputStream(i);
    }

    void insertDataToPrefetch(int v) {
      prefetchData.addFirst(v);
    }

    boolean equalString(String str) throws IOException {
      byte[] strBytes = str.getBytes(Charset.forName("UTF-8"));
      while (prefetchData.size() < strBytes.length) {
        prefetchData.add(input.read());
      }
      if (dataBytes == null
              || dataBytes.length < strBytes.length) {
        dataBytes = new byte[strBytes.length];
      }
      for (int i = 0; i != dataBytes.length; ++i) {
        dataBytes[i] = (byte) (int) prefetchData.get(i);
      }
      return Bytes.compareTo(strBytes, 0, strBytes.length,
              dataBytes, 0, strBytes.length) == 0;
    }

    int read() throws IOException {
      if (prefetchData.isEmpty()) {
        return input.read();
      } else {
        return prefetchData.remove(0);
      }
    }

    /**
     * Nothing to close, because the input is got from
     * {@link com.sun.net.httpserver.HttpExchange}.
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
    }
  }
}
