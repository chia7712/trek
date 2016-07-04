package com.spright.trek.mapping;

import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.spright.trek.exception.UriParseIOException;
import com.spright.trek.utils.BaseBuilder;
import com.spright.trek.utils.TrekUtils;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class AccountInfo {

  public static void write(final JsonWriter writer, final AccountInfo info) throws IOException {
    writer.beginObject()
            .name("id").value(info.getId().orElse(""))
            .name("domain").value(info.getDomain().orElse(""))
            .name("user").value(info.getUser().orElse(""))
            .name("password").value(info.getPassword().orElse(""))
            .name("host").value(info.getHost().orElse(""))
            .name("port").value(info.getPort().orElse(0))
            .endObject();
  }

  public static AccountInfo read(final JsonReader reader) throws IOException {
    AccountInfo.Builder builder = AccountInfo.newBuilder();
    reader.beginObject();
    while (reader.hasNext()) {
      String name = reader.nextName();
      if (name.equalsIgnoreCase("id")) {
        builder.setId(reader.nextString());
      } else if (name.equalsIgnoreCase("domain")) {
        builder.setDomain(reader.nextString());
      } else if (name.equalsIgnoreCase("user")) {
        builder.setUser(reader.nextString());
      } else if (name.equalsIgnoreCase("password")) {
        builder.setPassword(reader.nextString());
      } else if (name.equalsIgnoreCase("host")) {
        builder.setHost(reader.nextString());
      } else if (name.equalsIgnoreCase("port")) {
        builder.setPort(reader.nextInt());
      } else {
        reader.skipValue();
      }
    }
    reader.endObject();
    return builder.build();
  }
  public static final AccountInfo EMPTY = new AccountInfo(null, null, null, null, null, -1);

  public static Builder newBuilder() {
    return new Builder();
  }

  public enum Field {
    ID("id"),
    DOMAIN("domain"),
    USER("user"),
    PASSWORD("password"),
    HOST("host"),
    PORT("port");
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
  };
  private final String id;
  private final String domain;
  private final String user;
  private final String password;
  private final String host;
  private final int port;

  private AccountInfo(final String id, final String domain,
          final String user, final String password,
          final String host, final int port) {
    this.id = id;
    this.domain = domain;
    this.user = user;
    this.password = password;
    this.host = host;
    this.port = port;
  }

  public boolean isEmpty() {
    return id == null
            && domain == null
            && user == null
            && password == null
            && host == null
            && port < 0;
  }

  public final Optional<String> getId() {
    return Optional.ofNullable(id);
  }

  public final Optional<String> getDomain() {
    return Optional.ofNullable(domain);
  }

  public final Optional<String> getUser() {
    return Optional.ofNullable(user);
  }

  public final Optional<String> getPassword() {
    return Optional.ofNullable(password);
  }

  public final Optional<String> getHost() {
    return Optional.ofNullable(host);
  }

  public final Optional<Integer> getPort() {
    return port < 0 ? Optional.empty() : Optional.of(port);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj instanceof AccountInfo) {
      AccountInfo other = (AccountInfo) obj;
      return AccountInfoQuery.DEFAULT_COMPARATOR.compare(this, other) == 0;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 97 * hash + Objects.hashCode(this.id);
    hash = 97 * hash + Objects.hashCode(this.domain);
    hash = 97 * hash + Objects.hashCode(this.user);
    hash = 97 * hash + Objects.hashCode(this.password);
    hash = 97 * hash + Objects.hashCode(this.host);
    hash = 97 * hash + Objects.hashCode(this.port);
    return hash;
  }

  @Override
  public String toString() {
    return toString(true);
  }

  public final String toString(boolean useId) {
    StringBuilder str = new StringBuilder();
    if (useId && getId().isPresent()) {
      str.append("$")
              .append(getId().get());
    } else {
      if (getDomain().isPresent()) {
        str.append(getDomain().get())
                .append(";");
      }
      if (getUser().isPresent() && getPassword().isPresent()) {
        str.append(getUser().get())
                .append(":")
                .append(getPassword().get())
                .append("@");
      }
      if (getHost().isPresent()) {
        str.append(getHost().get());
      }
      if (getPort().isPresent()) {
        str.append(":")
                .append(getPort().get());
      }
    }
    return str.toString();
  }

  public static class Builder extends BaseBuilder {

    private String id = null;
    private String domain = null;
    private String user = null;
    private String password = null;
    private String host = null;
    private int port = -1;

    private Builder() {
    }

    public Builder set(final AccountInfo value) {
      if (isValid(value)) {
        id = value.getId().orElse(null);
        domain = value.getDomain().orElse(null);
        user = value.getUser().orElse(null);
        password = value.getPassword().orElse(null);
        host = value.getHost().orElse(null);
        port = value.getPort().orElse(null);
      }
      return this;
    }

    public Builder setId(final String v) {
      if (isValid(v)) {
        id = v;
      }
      return this;
    }

    public Builder setDomain(final String v) {
      if (isValid(v)) {
        domain = v;
      }
      return this;
    }

    public Builder setUser(final String v) {
      if (isValid(v)) {
        user = v;
      }
      return this;
    }

    public Builder setEncodedPassword(final String v) throws UriParseIOException {
      if (isValid(v)) {
        password = TrekUtils.decode(v);
      }
      return this;
    }

    public Builder setPassword(final String v) {
      if (isValid(v)) {
        password = v;
      }
      return this;
    }

    public Builder setHost(final String v) {
      if (isValid(v)) {
        host = v;
      }
      return this;
    }

    public Builder setPort(final int v) {
      if (isValid(v)) {
        port = v;
      }
      return this;
    }

    public AccountInfo build() {
      return new AccountInfo(id, domain, user, password, host, port);
    }
  }

}
