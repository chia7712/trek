package com.spright.trek.datasystem;

import com.spright.trek.datasystem.request.DataType;
import com.spright.trek.datasystem.request.DataInfo;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.Optional;
import com.spright.trek.datasystem.request.DataInfoQuery;
import com.spright.trek.datasystem.request.ReadDataRequest;
import com.spright.trek.datasystem.request.WriteDataRequest;
import com.spright.trek.exception.HdfsIOException;
import com.spright.trek.datasystem.request.UriRequest;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.query.IteratorUtils;
import com.spright.trek.utils.TrekUtils;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HdfsDataSystem extends DataSystem {

  private static final Log LOG = LogFactory.getLog(HdfsDataSystem.class);
  private final FileSystem fs;

  public HdfsDataSystem(final Configuration config) throws Exception {
    super(config, Protocol.HDFS);
    fs = FileSystem.get(config);
  }

  @Override
  protected void close() throws IOException {
  }

  @Override
  protected InputChannel internalOpen(ReadDataRequest request) throws IOException {
    Path path = new Path(request.getPath().toString());
    checkDataExisted(request, fs.exists(path));
    DataInfo info = toDataInfo(request, fs.getFileLinkStatus(path), fs);
    checkDataType(info.getType(), DataType.FILE);
    return new InputChannel() {
      private final InputStream input = fs.open(path);

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
  public void internalDelete(final DataInfo info) throws IOException {
    Path path = new Path(info.getUriRequest().getPath().toString());
    checkDataType(info.getType(), DataType.FILE);
    if (!fs.delete(path, true)) {
      throw new IOException("Failed to delete:" + path);
    }
  }

  @Override
  protected OutputChannel internalCreate(final WriteDataRequest request) throws IOException {
    Path realPath = new Path(request.getPath().toString());
    Path tmpPath = new Path(createTmpPath(realPath.toString()));
    return new OutputChannel() {
      private boolean hasMove = false;
      private final Optional<Long> ts = request.getExpectedTime();
      private final OutputStream output = fs.create(tmpPath);

      @Override
      public WriteDataRequest getRequest() {
        return request;
      }

      @Override
      public OutputStream getOutputStream() {
        return output;
      }

      @Override
      public void close() throws IOException {
        output.close();
        boolean exist = fs.exists(realPath);
        if (exist && !fs.isFile(realPath)) {
          throw new IOException("The dst is existed and it is not file");
        }
        if (exist && !fs.delete(realPath, true)) {
          throw new IOException("Failed to delete old data:" + realPath);
        }
        hasMove = fs.rename(tmpPath, realPath);
        if (!hasMove) {
          throw new HdfsIOException("Failed to move path from "
                  + tmpPath + " to " + realPath);
        }
        if (ts.isPresent()) {
          fs.setTimes(realPath, ts.get(), ts.get());
        }
      }

      @Override
      public void recover() throws IOException {
        if (hasMove) {
          fs.delete(realPath, true);
        } else {
          fs.delete(tmpPath, true);
        }
      }
    };
  }

  @Override
  protected CloseableIterator<DataInfo> internalList(final DataInfoQuery request) throws IOException {
    final Path path = new Path(request.getPath().toString());
    FileStatus status = fs.getFileStatus(path);
    checkDataType(toDataInfo(request, status, fs).getType(), DataType.DIRECTORY);
    List<DataInfo> infos = new LinkedList<>();
    for (FileStatus s : fs.listStatus(path)) {
      infos.add(toDataInfo(request.appendName(s.getPath().getName()), s, fs));
    }
    return IteratorUtils.wrap(infos.iterator());
  }

  private static DataInfo toDataInfo(final UriRequest request,
          final FileStatus status, final FileSystem fs) throws IOException {
    DataInfo.Builder builder = DataInfo.newBuilder();
    if (status.isFile()) {
      builder.setType(DataType.FILE)
              .setSize(status.getLen());
    } else if (status.isDirectory()) {
      builder.setType(DataType.DIRECTORY)
              .setSize(0);
    } else {
      builder.setType(DataType.OTHERS)
              .setSize(status.getLen());
    }
    return builder.setOwners(TrekUtils.findOwners(fs, status))
            .setRequest(request)
            .setUploadTime(status.getModificationTime())
            .build();
  }
}
