package com.spright.trek.datasystem;

import com.spright.trek.datasystem.request.DataType;
import com.spright.trek.datasystem.request.DataOwner;
import com.spright.trek.datasystem.request.DataInfo;
import com.spright.trek.datasystem.request.DataInfoQuery;
import com.spright.trek.datasystem.request.ReadDataRequest;
import com.spright.trek.datasystem.request.WriteDataRequest;
import com.spright.trek.exception.FileIOException;
import com.spright.trek.DConstants;
import com.spright.trek.utils.TrekUtils;
import com.spright.trek.datasystem.request.UriRequest;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.query.IteratorUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

public class FileDataSystem extends DataSystem {

  public FileDataSystem(final Configuration config) throws Exception {
    super(config, Protocol.FILE);
  }

  @Override
  protected void close() throws IOException {
  }

  @Override
  protected InputChannel internalOpen(ReadDataRequest request) throws IOException {
    File file = new File(request.getPath().toString());
    checkDataExisted(request, file.exists());
    DataInfo info = toDataInfo(request, file);
    checkDataType(info.getType(), DataType.FILE);
    return new InputChannel() {
      private final InputStream input = new FileInputStream(file);

      @Override
      public void close() throws IOException {
        input.close();
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
    File realFile = new File(request.getPath().toString());
    File tmpFile = new File(createTmpPath(realFile.getCanonicalPath()));
    return new OutputChannel() {
      private final Optional<Long> ts = request.getExpectedTime();
      private final OutputStream output = new FileOutputStream(tmpFile);
      private boolean hasMove = false;

      @Override
      public WriteDataRequest getRequest() {
        return request;
      }

      @Override
      public void close() throws IOException {
        output.close();
        boolean exist = realFile.exists();
        if (exist && !realFile.isFile()) {
          throw new IOException("The dst is existed and it is not file");
        }
        if (exist && !realFile.delete()) {
          throw new IOException("Failed to overwrite " + realFile);
        }
        hasMove = tmpFile.renameTo(realFile);
        if (!hasMove) {
          throw new IOException("Failed to move local file from "
                  + tmpFile.getAbsolutePath()
                  + " to " + realFile.getAbsolutePath());
        }
        if (!ts.map(t -> realFile.setLastModified(t)).orElse(true)) {
          throw new FileIOException("Failed to set modified time");
        }
      }

      @Override
      public OutputStream getOutputStream() {
        return output;
      }

      @Override
      public void recover() throws IOException {
        if (hasMove) {
          if (!realFile.delete()) {
            throw new IOException("Failed to remove file : "
                    + realFile.getAbsolutePath());
          }
        } else if (!tmpFile.delete()) {
          throw new IOException("Failed to remove tmp file : "
                  + tmpFile.getAbsolutePath());
        }
      }
    };
  }

  @Override
  protected CloseableIterator<DataInfo> internalList(final DataInfoQuery request) throws IOException {
    final String host = TrekUtils.getHostname();
    File file = new File(request.getPath().toString());
    checkDataExisted(request, file.exists());
    checkDataType(toDataInfo(request, file).getType(), DataType.FILE);
    return IteratorUtils.wrap(
            IteratorUtils.wrap(Arrays.asList(file.listFiles()).iterator()),
            (File f) -> toDataInfo(request.appendName(f.getName()), f));
  }

  private static DataInfo toDataInfo(final UriRequest request, final File file) {
    DataInfo.Builder builder = DataInfo.newBuilder();
    if (file.isFile()) {
      builder.setType(DataType.FILE)
              .setSize(file.length());
    } else if (file.isDirectory()) {
      builder.setType(DataType.DIRECTORY)
              .setSize(0);
    } else {
      builder.setType(DataType.OTHERS)
              .setSize(file.length());
    }
    return builder.setOwner(DataOwner.newSingleOwner(TrekUtils.getHostname()))
            .setRequest(request)
            .setUploadTime(file.lastModified())
            .build();
  }

  @Override
  protected void internalDelete(DataInfo info) throws IOException {
    File file = new File(info.getUriRequest().getPath().toString());
    if (!file.delete()) {
      throw new FileIOException("Failed to delete file:" + file);
    }
  }
}
