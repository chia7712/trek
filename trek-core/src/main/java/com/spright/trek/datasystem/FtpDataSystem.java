package com.spright.trek.datasystem;

import com.spright.trek.DConstants;
import com.spright.trek.datasystem.request.DataType;
import com.spright.trek.datasystem.request.DataOwner;
import com.spright.trek.datasystem.request.DataInfo;
import com.spright.trek.mapping.AccountInfo;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import com.spright.trek.datasystem.request.DataInfoQuery;
import com.spright.trek.datasystem.request.ReadDataRequest;
import com.spright.trek.datasystem.request.UriRequest;
import com.spright.trek.datasystem.request.WriteDataRequest;
import com.spright.trek.exception.FtpIOException;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.query.IteratorUtils;
import java.io.Closeable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import org.apache.commons.io.FilenameUtils;

/**
 * Ftp data system.
 */
public class FtpDataSystem extends DataSystem {

  private static final Log LOG = LogFactory.getLog(FtpDataSystem.class);

  public FtpDataSystem(final Configuration config) throws Exception {
    super(config, Protocol.FTP);
  }

  @Override
  public InputChannel internalOpen(final ReadDataRequest request) throws IOException {
    FtpConnection ftp = new FtpConnection(request);
    try {
      DataInfo info = ftp.getDataInfo(request);
      checkDataType(info.getType(), DataType.FILE);
      InputStream input = ftp.open(request.getPath().toString());
      int reply = ftp.getReplyCode();
      if (reply != 150 && !FTPReply.isPositiveCompletion(reply)) {
        LOG.info("error reply");
        throw new IOException("Failed to open ftp input stream");
      }
      return new InputChannel() {
        @Override
        public void close() throws Exception {
          try {
            input.close();
            if (!ftp.completePendingCommand()) {
              throw new FtpIOException("Failed to complete write ftp file");
            }
          } finally {
            ftp.close();
          }
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
    } catch (IOException e) {
      ftp.close();
      throw e;
    }
  }

  @Override
  public OutputChannel internalCreate(final WriteDataRequest request) throws IOException {
    FtpConnection ftp = new FtpConnection(request);
    try {
      if (!ftp.supportMFMT() && request.getExpectedTime().isPresent()) {
        throw new FtpIOException("The ftp server does not support MFMT command");
      }
      final String path = request.getPath().toString();
      String tmpPath = createTmpPath(path);
      OutputStream output = ftp.create(tmpPath);
      return new OutputChannel() {
        private final Optional<Long> ts = request.getExpectedTime();
        private boolean hasMove = false;

        @Override
        public WriteDataRequest getRequest() {
          return request;
        }

        @Override
        public void recover() throws IOException {
          try {
            if (hasMove) {
              ftp.deleteFile(path);
            } else {
              ftp.deleteFile(tmpPath);
            }
          } finally {
            ftp.close();
          }
        }

        @Override
        public void close() throws IOException {
          output.close();
          if (!ftp.completePendingCommand()) {
            throw new FtpIOException("Failed to complete write ftp file");
          }
          boolean exist = ftp.exist(path);
          if (exist && !ftp.isFile(request)) {
            throw new IOException("The dst is existed and it is not file");
          }
          if (exist) {
            ftp.deleteFile(path);
          }
          ftp.rename(tmpPath, path);
          hasMove = true;
          Optional<String> ftpTs = ts.map(t
                  -> new SimpleDateFormat(DConstants.FTP_TIME_FORMAT).format(t));
          if (ftpTs.isPresent()) {
            ftp.setModificationTime(path, ftpTs.get());
          }
          ftp.close();
        }

        @Override
        public OutputStream getOutputStream() {
          return output;
        }
      };
    } catch (IOException e) {
      ftp.close();
      throw e;
    }
  }

  @Override
  protected CloseableIterator<DataInfo> internalList(final DataInfoQuery request) throws IOException {
    try (FtpConnection ftp = new FtpConnection(request)) {
      String path = request.getPath().toString();
      String host = DataSystem.getHostOrThrow(request);
      DataInfo info = ftp.getDataInfo(request);
      checkDataType(info.getType(), DataType.DIRECTORY);
      return IteratorUtils.wrap(
              IteratorUtils.wrap(Arrays.asList(ftp.list(path)).iterator()),
              (FTPFile f) -> toDataInfo(request.appendName(f.getName()), host, f));
    }
  }

  @Override
  protected void internalDelete(final DataInfo info) throws IOException {
    try (FtpConnection ftp = new FtpConnection(info.getUriRequest())) {
      String path = info.getUriRequest().getPath().toString();
      switch (info.getType()) {
        case DIRECTORY:
          if (path.equalsIgnoreCase("/")) {
            ftp.deleteDirectory(path, "");
          } else if (path.endsWith("/")) {
            ftp.deleteDirectory(path.substring(0, path.length() - 1), "");
          } else {
            ftp.deleteDirectory(path, "");
          }
          break;
        case FILE:
        default:
          ftp.deleteFile(path);
          break;
      }
    }
  }

  @Override
  protected void close() throws Exception {
  }

  private static class FtpConnection implements Closeable {

    private final FTPClient client;
    private final boolean supportMFMT;
    private final boolean supportMLST;

    FtpConnection(final UriRequest request) throws IOException {
      AccountInfo info = request.getAccountInfo();
      String host = DataSystem.getHostOrThrow(request);
      client = new FTPClient();
      try {
        client.setControlKeepAliveTimeout(Long.MAX_VALUE);
        client.connect(host);
        int reply = client.getReplyCode();
        if (!FTPReply.isPositiveCompletion(reply)) {
          client.disconnect();
          throw new IOException("FTP server refused connection");
        }
        if (info.getUser().isPresent() && info.getPassword().isPresent()) {
          client.login(info.getUser().get(), info.getPassword().get());
        }
        /**
         * Default value is FTP.ASCII_FILE_TYPE, it will introduce to lose data.
         * We must change the file type here.
         */
        client.setFileType(FTP.BINARY_FILE_TYPE);
        String helps = client.listHelp();
        supportMFMT = helps.contains("MFMT");
        supportMLST = helps.contains("MLST");
      } catch (IOException e) {
        client.disconnect();
        throw e;
      }
    }

    void deleteFile(final String path) throws IOException {
      if (!client.deleteFile(path)) {
        throw new FtpIOException("Failed to delete:" + path);
      }
    }

    void deleteDirectory(String parentDir, String currentDir) throws IOException {
      String dirToList;
      if (currentDir.length() == 0) {
        dirToList = parentDir;
      } else {
        dirToList = parentDir + "/" + currentDir;
      }
      FTPFile[] subFiles = client.listFiles(dirToList);
      if (subFiles != null && subFiles.length > 0) {
        for (FTPFile aFile : subFiles) {
          String currentFileName = aFile.getName();
          if (currentFileName.equals(".") || currentFileName.equals("..")) {
            // skip parent directory and the directory itself
            continue;
          }
          String filePath;
          if (currentDir.length() == 0) {
            filePath = parentDir + "/" + currentFileName;
          } else {
            filePath = parentDir + "/" + currentDir + "/" + currentFileName;
          }
          if (aFile.isDirectory()) {
            deleteDirectory(dirToList, currentFileName);
          } else if (!client.deleteFile(filePath)) {
            throw DataSystem.getFailedToDeleteFileException(filePath);
          }
        }
      }
      if (!client.removeDirectory(dirToList)) {
        throw DataSystem.getFailedToDeleteFileException(dirToList);
      }
    }

    int getReplyCode() {
      return client.getReplyCode();
    }

    boolean completePendingCommand() throws IOException {
      return client.completePendingCommand();
    }

    FTPFile[] list(final String path) throws IOException {
      return client.listFiles();
    }

    boolean exist(final String path) throws IOException {
      if (!supportMLST()) {
        throw new IOException("The ftp server is unsupported to MLST");
      }
      return client.mlistFile(path) != null;
    }

    boolean isFile(final UriRequest request) throws IOException {
      return getDataInfo(request).getType() == DataType.FILE;
    }

    DataInfo getDataInfo(final UriRequest request) throws IOException {
      if (!supportMLST()) {
        throw new IOException("The ftp server is unsupported to MLST");
      }
      String host = DataSystem.getHostOrThrow(request);
      FTPFile f = client.mlistFile(request.getPath().toString());
      if (f != null) {
        return toDataInfo(request, host, f);
      }
      throw new IOException("Failed to retrieve the info from:" + request.getPath().toString());
    }

    void rename(final String oriPath, final String dstPath) throws IOException {
      if (!client.rename(oriPath, dstPath)) {
        throw new FtpIOException("Failed to move ftp file from:" + oriPath + " to:" + dstPath);
      }
    }

    void setModificationTime(final String path, final String timeString) throws IOException {
      if (!client.setModificationTime(path, timeString)) {
        throw new FtpIOException("Failed to set modification time");
      }
    }

    boolean supportMLST() {
      return supportMLST;
    }

    boolean supportMFMT() {
      return supportMFMT;
    }

    InputStream open(final String path) throws IOException {
      return client.retrieveFileStream(path);
    }

    OutputStream create(final String path) throws IOException {
      return client.storeFileStream(path);
    }

    @Override
    public void close() throws IOException {
      client.disconnect();
    }

  }

  private static DataInfo toDataInfo(final UriRequest request, final String host,
          final FTPFile file) {
    DataInfo.Builder builder = DataInfo.newBuilder();
    if (file.isFile()) {
      builder.setType(DataType.FILE)
              .setSize(file.getSize());
    } else if (file.isDirectory()) {
      builder.setType(DataType.DIRECTORY)
              .setSize(0);
    } else {
      builder.setType(DataType.OTHERS)
              .setSize(file.getSize());
    }
    return builder.setOwner(DataOwner.newSingleOwner(host))
            .setRequest(request)
            .setUploadTime(file.getTimestamp().getTimeInMillis())
            .build();
  }
}
