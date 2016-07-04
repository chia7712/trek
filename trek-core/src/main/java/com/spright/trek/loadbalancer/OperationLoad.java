package com.spright.trek.loadbalancer;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.spright.trek.DConstants;
import com.spright.trek.utils.BaseBuilder;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class OperationLoad {

  public static void write(final JsonWriter writer, final OperationLoad load) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    writer.beginObject()
            .name("host").value(load.getHost())
            .name("type").value(load.getType().name())
            .name("undealtCount").value(load.getUndealtCount())
            .name("undealtBytes").value(load.getUndealtBytes())
            .name("totalCount").value(load.getTotalCount())
            .name("totalBytes").value(load.getTotalBytes())
            .name("firstCall").value(sdf.format(new Date(load.getFirstCall())))
            .name("lastCall").value(sdf.format(new Date(load.getLastCall())))
            .endObject();
  }

  public static OperationLoad read(final JsonReader reader) throws IOException, ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    OperationLoad.Builder builder = OperationLoad.newBuilder();
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equalsIgnoreCase("host")) {
        builder.setHost(reader.nextString());
      } else if (name.equalsIgnoreCase("type")) {
        builder.setType(Operation.valueOf(reader.nextString()));
      } else if (name.equalsIgnoreCase("undealtCount")) {
        builder.setUndealtCount(reader.nextLong());
      } else if (name.equalsIgnoreCase("undealtBytes")) {
        builder.setUndealtBytes(reader.nextLong());
      } else if (name.equalsIgnoreCase("totalCount")) {
        builder.setTotalCount(reader.nextLong());
      } else if (name.equalsIgnoreCase("totalBytes")) {
        builder.setTotalBytes(reader.nextLong());
      } else if (name.equalsIgnoreCase("firstCall")) {
        builder.setFirstCall(sdf.parse(reader.nextString()).getTime());
      } else if (name.equalsIgnoreCase("lastCall")) {
        builder.setLastCall(sdf.parse(reader.nextString()).getTime());
      } else {
        reader.skipValue();
      }
    }
    reader.endObject();
    return builder.build();
  }

  public static OperationLoad read(final DataInput input) throws IOException {
    return OperationLoad.newBuilder()
            .setHost(input.readUTF())
            .setType(Operation.find(input.readByte()).orElseThrow(() -> new IOException("Unknown op code")))
            .setUndealtBytes(input.readLong())
            .setUndealtCount(input.readLong())
            .setTotalBytes(input.readLong())
            .setTotalCount(input.readLong())
            .setFirstCall(input.readLong())
            .setLastCall(input.readLong())
            .build();
  }

  public static void write(final DataOutput output, OperationLoad load) throws IOException {
    output.writeUTF(load.getHost());
    output.writeByte(load.getType().getCode());
    output.writeLong(load.getUndealtBytes());
    output.writeLong(load.getUndealtCount());
    output.writeLong(load.getTotalBytes());
    output.writeLong(load.getTotalCount());
    output.writeLong(load.getFirstCall());
    output.writeLong(load.getLastCall());
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  private final String host;
  private final Operation type;
  private final long undealtCount;
  private final long undealtBytes;
  private final long totalCount;
  private final long totalBytes;
  private final long firstCall;
  private final long lastCall;

  public OperationLoad(final OperationLoad ref) {
    host = ref.getHost();
    type = ref.getType();
    undealtCount = ref.getUndealtCount();
    undealtBytes = ref.getUndealtBytes();
    totalCount = ref.getTotalCount();
    totalBytes = ref.getTotalBytes();
    firstCall = ref.getFirstCall();
    lastCall = ref.getLastCall();
  }

  public OperationLoad(final String host, final Operation type, final long undealtCount,
          final long undealtBytes, final long totalCount, final long totalBytes,
          final long firstCall, final long lastCall) {
    this.host = host;
    this.type = type;
    this.undealtCount = undealtCount;
    this.undealtBytes = undealtBytes;
    this.totalCount = totalCount;
    this.totalBytes = totalBytes;
    this.firstCall = firstCall;
    this.lastCall = lastCall;
  }

  public String getHost() {
    return host;
  }

  public Operation getType() {
    return type;
  }

  public long getUndealtCount() {
    return undealtCount;
  }

  public long getTotalCount() {
    return totalCount;
  }

  public long getUndealtBytes() {
    return undealtBytes;
  }

  public long getTotalBytes() {
    return totalBytes;
  }

  public long getFirstCall() {
    return firstCall;
  }

  public long getLastCall() {
    return lastCall;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    OperationLoad other = (OperationLoad) obj;
    return other.getFirstCall() == getFirstCall()
            && other.getHost().equals(getHost())
            && other.getLastCall() == getLastCall()
            && other.getTotalBytes() == getTotalBytes()
            && other.getTotalCount() == getTotalCount()
            && other.getType() == getType()
            && other.getUndealtBytes() == getUndealtBytes()
            && other.getUndealtCount() == getUndealtCount();
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 47 * hash + Objects.hashCode(this.host);
    hash = 47 * hash + Objects.hashCode(this.type);
    hash = 47 * hash + (int) (this.undealtCount ^ (this.undealtCount >>> 32));
    hash = 47 * hash + (int) (this.undealtBytes ^ (this.undealtBytes >>> 32));
    hash = 47 * hash + (int) (this.totalCount ^ (this.totalCount >>> 32));
    hash = 47 * hash + (int) (this.totalBytes ^ (this.totalBytes >>> 32));
    hash = 47 * hash + (int) (this.firstCall ^ (this.firstCall >>> 32));
    hash = 47 * hash + (int) (this.lastCall ^ (this.lastCall >>> 32));
    return hash;
  }

  public static class Builder extends BaseBuilder {

    private String host;
    private Operation type;
    private long undealtCount = -1;
    private long undealtBytes = -1;
    private long totalCount = -1;
    private long totalBytes = -1;
    private long firstCall = -1;
    private long lastCall = -1;

    private Builder() {
    }

    public Builder setHost(final String v) {
      if (isValid(v)) {
        host = v;
      }
      return this;
    }

    public Builder setType(final Operation v) {
      if (isValid(v)) {
        type = v;
      }
      return this;
    }

    public Builder setUndealtCount(final long v) {
      if (isValid(v)) {
        undealtCount = v;
      }
      return this;
    }

    public Builder setUndealtBytes(final long v) {
      if (isValid(v)) {
        undealtBytes = v;
      }
      return this;
    }

    public Builder setTotalCount(final long v) {
      if (isValid(v)) {
        totalCount = v;
      }
      return this;
    }

    public Builder setTotalBytes(final long v) {
      if (isValid(v)) {
        totalBytes = v;
      }
      return this;
    }

    public Builder setFirstCall(final long v) {
      if (isValid(v)) {
        firstCall = v;
      }
      return this;
    }

    public Builder setLastCall(final long v) {
      if (isValid(v)) {
        lastCall = v;
      }
      return this;
    }

    public OperationLoad build() {
      checkNull(host, "host");
      checkNull(type, "type");
      checkNull(undealtCount, "undealtCount");
      checkNull(undealtBytes, "undealtBytes");
      checkNull(totalCount, "totalCount");
      checkNull(totalBytes, "totalBytes");
      checkNull(firstCall, "firstCall");
      checkNull(lastCall, "lastCall");
      return new OperationLoad(host, type, undealtCount, undealtBytes,
              totalCount, totalBytes, firstCall, lastCall);
    }
  }
}
