package com.spright.trek.web;

import com.google.gson.stream.JsonReader;
import com.spright.trek.task.AccessStatus;
import com.spright.trek.task.TaskState;
import com.spright.trek.DConstants;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class AccessTest {

  private static final String BOUNDARY = "--------";
  private static final String LINE_END = "\r\n";
  private static final Configuration CONFIG = new Configuration();

  public AccessTest() {
  }

  @BeforeClass
  public static void setUpClass() {
    CONFIG.setBoolean(DConstants.ENABLE_SINGLE_MODE, true);
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
  }

  private static void printErrorMessage(final HttpURLConnection httpConn) throws IOException {
    try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(httpConn.getErrorStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        System.out.println(line);
      }
    }
  }

  private static String toUnixPath(String path) {
    int windowIndex = path.indexOf(":\\");
    if (windowIndex != -1) {
      path = path.substring(windowIndex + ":\\".length());
    }
    return path.replaceAll("\\\\", "/");
  }

  private static String creatTempPath() throws IOException {
    File f = File.createTempFile("LocalToFileTest", null);
    return toUnixPath(f.getAbsolutePath());
  }

  private static File creatTestFile(final long size, final byte value) throws IOException {
    File f = File.createTempFile("LocalToFileTest", null);
    byte[] buf = new byte[1024 * 4];
    for (int i = 0; i != buf.length; ++i) {
      buf[i] = value;
    }
    try (OutputStream output = new FileOutputStream(f)) {
      long remaining = size;
      while (remaining != 0) {
        if (remaining >= buf.length) {
          output.write(buf, 0, buf.length);
          remaining -= buf.length;
        } else {
          output.write(buf, 0, (int) remaining);
          remaining = 0;
        }
      }
    }
    return f;
  }

  @Test
  public void testFileToFile() throws MalformedURLException, Exception, ParseException {
    System.out.println("test file to file");
    try (WebServer server = new WebServer(CONFIG)) {
      long fileSize = 5 * 1024 * 1024;
      File inputFile = creatTestFile(fileSize, (byte) 0x11);
      String tmpOutput = creatTempPath();
      String request = "http://127.0.0.1:"
              + DConstants.DEFAULT_RESTFUL_SERVER_BINDING_PORT
              + "/trek/v1/data/access?from=file:///"
              + toUnixPath(inputFile.getAbsolutePath())
              + "&to=file:///"
              + tmpOutput;
      URL url = new URL(request);
      HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
      httpConn.setDoOutput(true);
      httpConn.setDoInput(true);
      httpConn.setRequestMethod("POST");
      httpConn.setRequestProperty("User-Agent", "CodeJava Agent");
      httpConn.setRequestProperty("Content-Type",
              "multipart/form-data; boundary=" + BOUNDARY);
      httpConn.connect();
      int responseCode = httpConn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        printErrorMessage(httpConn);
        throw new IOException("Failed to upload data to"
                + " http server with error code " + responseCode);
      }
      try (JsonReader reader = new JsonReader(new InputStreamReader(httpConn.getInputStream()))) {
        AccessStatus status = AccessStatus.read(reader);
        assertEquals(fileSize, status.getTransferredSize());
        assertEquals(fileSize, status.getExpectedSize());
        assertEquals(1.0, status.getProgress(), 0);
        assertEquals(TaskState.SUCCEED, status.getState());
      }
      httpConn.disconnect();
      File output = new File("/" + tmpOutput);
      assertEquals(true, output.exists());
      assertEquals(fileSize, output.length());
      try (InputStream input = new FileInputStream(output)) {
        int rval;
        while ((rval = input.read()) != -1) {
          assertEquals((byte) 0x11, (byte) rval);
        }
      }
      inputFile.delete();
    }
  }

  @Test
  public void testFileToLocal() throws MalformedURLException, Exception {
    System.out.println("test file to local");
    try (WebServer server = new WebServer(CONFIG)) {
      File inputFile = creatTestFile(12345, (byte) 0x11);
      String request = "http://127.0.0.1:"
              + DConstants.DEFAULT_RESTFUL_SERVER_BINDING_PORT
              + "/trek/v1/data/access?to=local:///test/aaa2.pdf&from=file:///"
              + toUnixPath(inputFile.getAbsolutePath());
      URL url = new URL(request);
      HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
      httpConn.setDoOutput(true);
      httpConn.setDoInput(true);
      httpConn.setRequestMethod("POST");
      httpConn.setRequestProperty("User-Agent", "CodeJava Agent");
      httpConn.setRequestProperty("Content-Type",
              "multipart/form-data; boundary=" + BOUNDARY);
      httpConn.connect();
      int count = 0;
      try (InputStream output = httpConn.getInputStream()) {
        int rval;
        while ((rval = output.read()) != -1) {
          ++count;
          assertEquals((byte) 0x11, (byte) rval);
        }
      }
      assertEquals(12345, count);
      httpConn.disconnect();
    }
  }

  @Test
  public void testLocalToFile() throws MalformedURLException, Exception, ParseException {
    System.out.println("test local to file");
    try (WebServer server = new WebServer(CONFIG)) {
      String tmpOutput = creatTempPath();
      String outputName = "abc.test";
      byte[] outputData = createData(1024, (byte) 0x11);
      final int repeatCount = 1024;
      String request = "http://127.0.0.1:"
              + DConstants.DEFAULT_RESTFUL_SERVER_BINDING_PORT
              + "/trek/v1/data/access?from=local:///test/aaa.pdf&to=file:///"
              + tmpOutput;
      final long totalSize = (long) outputData.length * (long) repeatCount;
      System.out.println("send request:" + request);
      URL url = new URL(request);
      HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
      httpConn.setDoOutput(true);
      httpConn.setDoInput(true);
      httpConn.setChunkedStreamingMode(2048);
      httpConn.setRequestMethod("POST");
      httpConn.setRequestProperty("User-Agent", "CodeJava Agent");
      httpConn.setRequestProperty("Content-Type",
              "multipart/form-data; boundary=" + BOUNDARY);
      httpConn.connect();
      try (DataOutputStream output = new DataOutputStream(httpConn.getOutputStream())) {
        output.writeBytes("--");
        output.writeBytes(BOUNDARY);
        output.writeBytes(LINE_END);
        output.writeBytes("Content-Disposition: form-data; filename=\""
                + outputName + "\";");
        output.writeBytes(LINE_END);
        output.writeBytes("Content-Type: application/octet-stream;");
        output.writeBytes(LINE_END);
        output.writeBytes("Content-length: " + String.valueOf((totalSize)));
        output.writeBytes(LINE_END);
        output.writeBytes(LINE_END);
        long currentTime = System.currentTimeMillis();
        for (int i = 0; i != repeatCount; ++i) {
          long nextTime = System.currentTimeMillis();
          if (nextTime - currentTime >= 2000) {
            currentTime = nextTime;
            System.out.println(((long) outputData.length * (long) i) / (1024 * 1024) + "/" + (totalSize / (1024 * 1024)) + "mb");
          }
          output.write(outputData, 0, outputData.length);
        }
        output.writeBytes(LINE_END);
        output.writeBytes("--");
        output.writeBytes(BOUNDARY);
        output.writeBytes("--");
        output.writeBytes(LINE_END);
        output.flush();
      }
      int responseCode = httpConn.getResponseCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        printErrorMessage(httpConn);
        throw new IOException("Failed to upload data to"
                + " http server with error code " + responseCode);
      }
      try (JsonReader reader = new JsonReader(new InputStreamReader(httpConn.getInputStream()))) {
        AccessStatus status = AccessStatus.read(reader);
        assertEquals(totalSize, status.getTransferredSize());
        assertEquals(totalSize, status.getExpectedSize());
        assertEquals(1.0, status.getProgress(), 0);
        assertEquals(TaskState.SUCCEED, status.getState());
      }
      httpConn.disconnect();
    }
  }

  private static byte[] createData(final int size, final byte value) throws IOException {
    byte[] buf = new byte[size];
    for (int i = 0; i != buf.length; ++i) {
      buf[i] = value;
    }
    return buf;
  }
}
