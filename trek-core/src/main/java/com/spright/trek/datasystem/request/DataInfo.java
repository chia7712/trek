package com.spright.trek.datasystem.request;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.spright.trek.DConstants;
import com.spright.trek.mapping.Mapping;
import com.spright.trek.utils.BaseBuilder;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Describe the data.
 */
public class DataInfo {

  private static String toString(final List<DataOwner> owners) {
    StringBuilder buf = new StringBuilder();
    owners.forEach(v -> buf.append(v).append(","));
    return buf.length() == 0 ? buf.toString() : buf.substring(0, buf.length() - 1);
  }

  private static List<DataOwner> toOwner(final String str) throws IOException {
    List<DataOwner> ownerw = new LinkedList<>();
    for (String s : str.split(",")) {
      final int index = s.indexOf(":");
      if (index == -1 && index != (s.length() - 1)) {
        throw new IOException("Invalid owner format:" + str);
      }
      try {
        ownerw.add(new DataOwner(s.substring(0, index), Double.valueOf(s.substring(index + 1))));
      } catch (NumberFormatException e) {
        throw new IOException(e);
      }
    }
    return ownerw;
  }

  public static void write(final JsonWriter writer, final DataInfo info) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    writer.beginObject()
            .name("uri").value(info.getUriRequest().toString())
            .name("size").value(info.getSize())
            .name("ts").value(sdf.format(new Date(info.getUploadTime())))
            .name("type").value(info.getType().name())
            .name("dataowner").value(toString(info.getDataOwners()))
            .endObject();
  }

  public static DataInfo read(final JsonReader reader, final Mapping mapping) throws IOException, ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    DataInfo.Builder builder = DataInfo.newBuilder();
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equalsIgnoreCase("uri")) {
        builder.setRequest(UriRequest.parse(reader.nextString(), mapping, null));
      } else if (name.equalsIgnoreCase("size")) {
        builder.setSize(reader.nextLong());
      } else if (name.equalsIgnoreCase("ts")) {
        builder.setUploadTime(sdf.parse(reader.nextString()).getTime());
      } else if (name.equalsIgnoreCase("type")) {
        builder.setType(DataType.valueOf(reader.nextString()));
      } else if (name.equalsIgnoreCase("dataowner")) {
        builder.setOwners(toOwner(reader.nextString()));
      } else {
        reader.skipValue();
      }
    }
    reader.endObject();
    return builder.build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public enum Field {
    UPLOAD_TIME("uploadtime"),
    SIZE("size"),
    TYPE("type"),
    NAME("name");
    private final String description;

    Field(final String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }

    public static Optional<Field> find(final String orderBy) {
      for (Field f : Field.values()) {
        if (f.getDescription().equalsIgnoreCase(orderBy)) {
          return Optional.of(f);
        }
      }
      return Optional.empty();
    }
  };
  private final UriRequest request;
  private final long size;
  private final long uploadTime;
  private final DataType type;
  private final List<DataOwner> owners;

  public DataInfo(final UriRequest request, final long size,
          final long uploadTime, final DataType type, final List<DataOwner> owners) {
    this.request = request;
    this.size = size;
    this.uploadTime = uploadTime;
    this.type = type;
    this.owners = owners;
  }

  public DataInfo(final DataInfo ref) {
    request = ref.getUriRequest();
    size = ref.getSize();
    uploadTime = ref.getUploadTime();
    type = ref.getType();
    owners = ref.getDataOwners();
  }

  public UriRequest getUriRequest() {
    return request;
  }

  /**
   * @return Data size in bytes.
   */
  public long getSize() {
    return size;
  }

  /**
   * @return The time of data to upload
   */
  public long getUploadTime() {
    return uploadTime;
  }

  /**
   * @return The data owners
   */
  public List<DataOwner> getDataOwners() {
    return new ArrayList<>(owners);
  }

  /**
   * @return The data type
   */
  public DataType getType() {
    return type;
  }

  public static class Builder extends BaseBuilder {

    private UriRequest request;
    private long size;
    private List<DataOwner> owners;
    private long ts;
    private DataType type;

    private Builder() {
    }

    public Builder setRequest(final UriRequest v) {
      if (isValid(v)) {
        request = v;
      }
      return this;
    }

    public Builder setSize(final long v) {
      if (isValid(v)) {
        size = v;
      }
      return this;
    }

    public Builder setOwner(final DataOwner v) {
      if (isValid(v)) {
        owners = Arrays.asList(v);
      }
      return this;
    }

    public Builder setOwners(final List<DataOwner> v) {
      if (isValid(v)) {
        owners = v;
      }
      return this;
    }

    public Builder setUploadTime(final long v) {
      if (isValid(v)) {
        ts = v;
      }
      return this;
    }

    public Builder setType(final DataType v) {
      if (isValid(v)) {
        type = v;
      }
      return this;
    }

    public DataInfo build() {
      checkNull(request, "request");
      checkNull(size, "size");
      checkNull(ts, "ts");
      checkNull(owners, "owner");
      checkNull(type, "type");
      return new DataInfo(request, size, ts, type, owners);
    }
  }
}
