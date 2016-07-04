package com.spright.trek.task;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.spright.trek.utils.BaseBuilder;
import com.spright.trek.DConstants;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;

public class AccessStatus {

  public static void write(final JsonWriter writer, final AccessStatus info) throws IOException {
    SimpleDateFormat sdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    writer.beginObject()
            .name(Field.ID.getDescription()).value(info.getId())
            .name(Field.REDIRECT_FROM.getDescription()).value(info.getRedirectFrom())
            .name(Field.SERVER_NAME.getDescription()).value(info.getServerName())
            .name(Field.CLIENT_NAME.getDescription()).value(info.getClientName())
            .name(Field.FROM.getDescription()).value(info.getFrom())
            .name(Field.TO.getDescription()).value(info.getTo())
            .name(Field.STATE.getDescription()).value(info.getState().name())
            .name(Field.PROGRESS.getDescription()).value(info.getProgress())
            .name(Field.START_TIME.getDescription()).value(sdf.format(new Date(info.getStartTime())))
            .name(Field.ELAPSED.getDescription()).value(sdf.format(new Date(info.getElapsed())))
            .name(Field.EXPECTED_SIZE.getDescription()).value(info.getExpectedSize())
            .name(Field.TRANSFERRED_SIZE.getDescription()).value(info.getTransferredSize())
            .endObject();
  }

  public static AccessStatus read(final JsonReader reader) throws IOException, ParseException {
    SimpleDateFormat sdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    AccessStatus.Builder builder = AccessStatus.newBuilder();
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equalsIgnoreCase(Field.ID.getDescription())) {
        builder.setId(reader.nextString());
      } else if (name.equalsIgnoreCase(Field.REDIRECT_FROM.getDescription())) {
        builder.setRedirectFrom(reader.nextString());
      } else if (name.equalsIgnoreCase(Field.SERVER_NAME.getDescription())) {
        builder.setServerName(reader.nextString());
      } else if (name.equalsIgnoreCase(Field.CLIENT_NAME.getDescription())) {
        builder.setClientName(reader.nextString());
      } else if (name.equalsIgnoreCase(Field.FROM.getDescription())) {
        builder.setFrom(reader.nextString());
      } else if (name.equalsIgnoreCase(Field.TO.getDescription())) {
        builder.setTo(reader.nextString());
      } else if (name.equalsIgnoreCase(Field.STATE.getDescription())) {
        builder.setTaskState(TaskState.valueOf(reader.nextString()));
      } else if (name.equalsIgnoreCase(Field.PROGRESS.getDescription())) {
        builder.setProgress(reader.nextDouble());
      } else if (name.equalsIgnoreCase(Field.START_TIME.getDescription())) {
        builder.setStartTime(sdf.parse(reader.nextString()).getTime());
      } else if (name.equalsIgnoreCase(Field.ELAPSED.getDescription())) {
        builder.setElapsed(sdf.parse(reader.nextString()).getTime());
      } else if (name.equalsIgnoreCase(Field.EXPECTED_SIZE.getDescription())) {
        builder.setExpectedSize(reader.nextLong());
      } else if (name.equalsIgnoreCase(Field.TRANSFERRED_SIZE.getDescription())) {
        builder.setTransferredSize(reader.nextLong());
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

  public static Builder newBuilder(final AccessStatus status) {
    return new Builder(status);
  }

  public enum Field {
    ID("id"),
    REDIRECT_FROM("redirectfrom"),
    SERVER_NAME("servername"),
    CLIENT_NAME("clientname"),
    FROM("from"),
    TO("to"),
    STATE("state"),
    PROGRESS("progress"),
    START_TIME("starttime"),
    ELAPSED("elapsed"),
    EXPECTED_SIZE("expectedsize"),
    TRANSFERRED_SIZE("transferredsize");
    private final String desc;

    Field(final String desc) {
      this.desc = desc;
    }

    public String getDescription() {
      return desc;
    }

    public static Optional<Field> find(final String orderBy) {
      for (Field f : Field.values()) {
        if (f.getDescription().equalsIgnoreCase(orderBy)) {
          return Optional.of(f);
        }
      }
      return Optional.empty();
    }
  }
  private final SimpleDateFormat sdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
  private final String id;
  private final String redirectFrom;
  private final String serverName;
  private final String clientName;
  private final String from;
  private final String to;
  private final TaskState state;
  private final double progress;
  private final long startTime;
  private final long elapsed;
  private final long expectedSize;
  private final long transferredSize;

  public AccessStatus(final AccessStatus ref) {
    this(ref.getId(), ref.getRedirectFrom(), ref.getServerName(),
            ref.getClientName(), ref.getFrom(), ref.getTo(),
            ref.getState(), ref.getProgress(), ref.getStartTime(),
            ref.getElapsed(), ref.getExpectedSize(), ref.getTransferredSize());
  }

  public AccessStatus(final String id, final String redirectFrom,
          final String serverName, final String clientName,
          final String from, final String to, final TaskState state,
          final double progress, final long startTime,
          final long elapsed, final long expectedSize,
          final long transferredSize) {
    this.id = id;
    this.redirectFrom = redirectFrom;
    this.serverName = serverName;
    this.clientName = clientName;
    this.from = from;
    this.to = to;
    this.state = state;
    this.progress = progress;
    this.startTime = startTime;
    this.elapsed = elapsed;
    this.expectedSize = expectedSize;
    this.transferredSize = transferredSize;
  }

  public String getId() {
    return id;
  }

  public String getRedirectFrom() {
    return redirectFrom;
  }

  public String getServerName() {
    return serverName;
  }

  public String getClientName() {
    return clientName;
  }

  public String getFrom() {
    return from;
  }

  public String getTo() {
    return to;
  }

  public TaskState getState() {
    return state;
  }

  public double getProgress() {
    return progress;
  }

  public long getStartTime() {
    return startTime;
  }

  public long getElapsed() {
    return elapsed;
  }

  public long getExpectedSize() {
    return expectedSize;
  }

  public long getTransferredSize() {
    return transferredSize;
  }

  public String formatStartTime() {
    return sdf.format(new Date(getStartTime()));
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof AccessStatus) {
      AccessStatus other = (AccessStatus) obj;
      return AccessStatusQuery.DEFAULT_COMPARATOR.compare(this, other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 11 * hash + Objects.hashCode(this.sdf);
    hash = 11 * hash + Objects.hashCode(this.id);
    hash = 11 * hash + Objects.hashCode(this.redirectFrom);
    hash = 11 * hash + Objects.hashCode(this.serverName);
    hash = 11 * hash + Objects.hashCode(this.clientName);
    hash = 11 * hash + Objects.hashCode(this.from);
    hash = 11 * hash + Objects.hashCode(this.to);
    hash = 11 * hash + Objects.hashCode(this.state);
    hash = 11 * hash + (int) (Double.doubleToLongBits(this.progress) ^ (Double.doubleToLongBits(this.progress) >>> 32));
    hash = 11 * hash + (int) (this.startTime ^ (this.startTime >>> 32));
    hash = 11 * hash + (int) (this.elapsed ^ (this.elapsed >>> 32));
    hash = 11 * hash + (int) (this.expectedSize ^ (this.expectedSize >>> 32));
    hash = 11 * hash + (int) (this.transferredSize ^ (this.transferredSize >>> 32));
    return hash;
  }

  public static class Builder extends BaseBuilder {

    private String id;
    private String redirectFrom;
    private String serverName;
    private String clientName;
    private String from;
    private String to;
    private TaskState state;
    private double progress;
    private long startTime;
    private long elapsed;
    private long expectedSize;
    private long transferredSize;

    private Builder() {
    }

    private Builder(final AccessStatus status) {
      redirectFrom = status.getRedirectFrom();
      serverName = status.getServerName();
      clientName = status.getClientName();
      startTime = status.getStartTime();
      elapsed = status.getElapsed();
      expectedSize = status.getExpectedSize();
      transferredSize = status.getTransferredSize();
      from = status.getFrom();
      to = status.getTo();
      state = status.getState();
      progress = status.getProgress();
      id = status.getId();
    }

    public Builder setId(final String v) {
      if (isValid(v)) {
        id = v;
      }
      return this;
    }

    public Builder setRedirectFrom(final String v) {
      if (isValid(v)) {
        redirectFrom = v;
      }
      return this;
    }

    public Builder setServerName(final String v) {
      if (isValid(v)) {
        serverName = v;
      }
      return this;
    }

    public Builder setClientName(final String v) {
      if (isValid(v)) {
        clientName = v;
      }
      return this;
    }

    public Builder setFrom(final String v) {
      if (isValid(v)) {
        from = v;
      }
      return this;
    }

    public Builder setTo(final String v) {
      if (isValid(v)) {
        to = v;
      }
      return this;
    }

    public Builder setProgress(final double v) {
      if (isValid(v)) {
        progress = v;
      }
      return this;
    }

    public Builder setTaskState(final TaskState v) {
      if (isValid(v)) {
        state = v;
      }
      return this;
    }

    public Builder setStartTime(final long v) {
      if (isValid(v)) {
        startTime = v;
      }
      return this;
    }

    public Builder setElapsed(final long v) {
      if (isValid(v)) {
        elapsed = v;
      }
      return this;
    }

    public Builder setExpectedSize(final long v) {
      if (isValid(v)) {
        expectedSize = v;
      }
      return this;
    }

    public Builder setTransferredSize(final long v) {
      if (isValid(v)) {
        transferredSize = v;
      }
      return this;
    }

    public AccessStatus build() {
      checkNull(id, "id");
      checkNull(redirectFrom, "redirectFrom");
      checkNull(serverName, "serverName");
      checkNull(clientName, "clientName");
      checkNull(from, "from");
      checkNull(to, "to");
      checkNull(state, "to");
      checkNull(progress, "progress", 1.0f);
      checkNull(startTime, "startTime");
      checkNull(elapsed, "elapsed");
      checkNull(startTime, "expectedSize");
      checkNull(transferredSize, "transferredSize");
      return new AccessStatus(id, redirectFrom, serverName, clientName,
              from, to, state, progress, startTime, elapsed, expectedSize,
              transferredSize);
    }
  }
}
