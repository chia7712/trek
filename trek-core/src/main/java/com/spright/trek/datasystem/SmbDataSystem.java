package com.spright.trek.datasystem;

import com.spright.trek.datasystem.request.DataType;
import com.spright.trek.datasystem.request.DataOwner;
import com.spright.trek.datasystem.request.DataInfo;
import com.spright.trek.DConstants;
import com.spright.trek.mapping.AccountInfo;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import jcifs.smb.NtlmPasswordAuthentication;
import jcifs.smb.SmbFile;
import jcifs.smb.SmbException;
import org.apache.hadoop.conf.Configuration;
import java.util.Optional;
import com.spright.trek.datasystem.request.DataInfoQuery;
import com.spright.trek.datasystem.request.UriRequest;
import com.spright.trek.datasystem.request.ReadDataRequest;
import com.spright.trek.datasystem.request.WriteDataRequest;
import com.spright.trek.exception.UriParseIOException;
import com.spright.trek.query.CloseableIterator;
import com.spright.trek.query.IteratorUtils;
import java.net.MalformedURLException;
import java.util.LinkedList;
import java.util.List;

public class SmbDataSystem extends DataSystem {

  public SmbDataSystem(final Configuration config) throws Exception {
    super(config, Protocol.SMB);
  }

  @Override
  protected InputChannel internalOpen(ReadDataRequest request) throws IOException {
    SmbFile file = new SmbFile(toUrlStringPath(request), toAuth(request));
    checkDataExisted(request, file.exists());
    DataInfo info = toDataInfo(request, file);
    checkDataType(info.getType(), DataType.FILE);
    return new InputChannel() {
      private final InputStream input = file.getInputStream();

      @Override
      public DataInfo getInfo() {
        return info;
      }

      @Override
      public InputStream getInputStream() {
        return input;
      }

      @Override
      public void close() throws IOException {
        input.close();
      }
    };
  }

  @Override
  protected OutputChannel internalCreate(final WriteDataRequest request) throws IOException {
    DataSystem.getHostOrThrow(request);
    NtlmPasswordAuthentication auth = toAuth(request);
    SmbFile realFile = new SmbFile(toUrlStringPath(request), auth);
    SmbFile tmpFile = new SmbFile(createTmpPath(realFile.getPath()), auth);
    return new OutputChannel() {
      private final Optional<Long> ts = request.getExpectedTime();
      private final OutputStream output = tmpFile.getOutputStream();
      private boolean hasMove = false;

      @Override
      public OutputStream getOutputStream() {
        return output;
      }

      @Override
      public void close() throws IOException {
        output.close();
        try {
          boolean exist = realFile.exists();
          if (exist && !realFile.isFile()) {
            throw new IOException("The dst is existed and it is not file");
          }
          if (exist) {
            realFile.delete();
          }
          tmpFile.renameTo(realFile);
          hasMove = true;
          if (ts.isPresent()) {
            realFile.setLastModified(ts.get());
          }
        } catch (SmbException e) {
          hasMove = false;
          throw e;
        }
      }

      @Override
      public void recover() throws IOException {
        if (hasMove) {
          realFile.delete();
        } else {
          tmpFile.delete();
        }
      }

      @Override
      public WriteDataRequest getRequest() {
        return request;
      }
    };
  }

  private static NtlmPasswordAuthentication toAuth(final UriRequest request) {
    AccountInfo account = request.getAccountInfo();
    String domain = account.getDomain().orElse(null);
    String user = account.getUser().orElse(null);
    String password = account.getPassword().orElse(null);
    NtlmPasswordAuthentication auth = new NtlmPasswordAuthentication(domain, user, password);
    return auth;
  }

  private static String toUrlStringPath(final UriRequest request)
          throws UriParseIOException {
    return "smb://"
            + DataSystem.getHostOrThrow(request)
            + request.getPath().toString();
  }

  @Override
  protected CloseableIterator<DataInfo> internalList(DataInfoQuery request) throws IOException {
    String path = toUrlStringPath(request);
    if (!path.endsWith("/")) {
      path = path + "/";
    }
    SmbFile smbFile = new SmbFile(path, toAuth(request));
    checkDataType(toDataInfo(request, smbFile).getType(), DataType.DIRECTORY);
    List<DataInfo> infos = new LinkedList<>();
    for (SmbFile f : smbFile.listFiles()) {
      infos.add(toDataInfo(request.appendName(f.getName()), f));
    }
    return IteratorUtils.wrap(infos.iterator());
  }

  private static DataInfo toDataInfo(final UriRequest request,
          final SmbFile file) throws IOException {
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
    return builder.setOwner(DataOwner.newSingleOwner(file.getServer()))
            .setRequest(request.appendName(file.getName()))
            .setUploadTime(file.getLastModified())
            .build();
  }

  @Override
  protected void close() throws Exception {
  }

  @Override
  protected void internalDelete(DataInfo info) throws IOException {
    String path = toUrlStringPath(info.getUriRequest());
    if (info.getType() == DataType.DIRECTORY
            && !path.endsWith("/")) {
      path += "/";
    }
    SmbFile file = new SmbFile(path, toAuth(info.getUriRequest()));
    file.delete();
  }
}
