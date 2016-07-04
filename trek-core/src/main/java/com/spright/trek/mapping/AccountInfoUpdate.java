package com.spright.trek.mapping;

import com.spright.trek.exception.UriParseIOException;
import com.spright.trek.DConstants;
import com.spright.trek.query.EmptyableInteger;
import com.spright.trek.query.EmptyableString;
import com.spright.trek.query.QueryUtils;
import com.spright.trek.utils.BaseBuilder;
import java.util.Map;

public class AccountInfoUpdate {

  public static Builder newBuilder() {
    return new Builder();
  }

  public static AccountInfoUpdate parse(Map<String, String> rawQuery) throws UriParseIOException {
    return AccountInfoUpdate.newBuilder()
            .setDomain(new EmptyableString(rawQuery.get(DConstants.URI_MAPPING_DOMAIN)))
            .setHost(new EmptyableString(rawQuery.get(DConstants.URI_MAPPING_HOST)))
            .setId(QueryUtils.checkEmptyString(rawQuery.get(DConstants.URI_MAPPING_ID)))
            .setPassword(new EmptyableString(rawQuery.get(DConstants.URI_MAPPING_PASSWORD)))
            .setPort(new EmptyableInteger(rawQuery.get(DConstants.URI_MAPPING_PORT)))
            .setUser(new EmptyableString(rawQuery.get(DConstants.URI_MAPPING_USER)))
            .build();
  }
  private final String id;
  private final EmptyableString domain;
  private final EmptyableString user;
  private final EmptyableString password;
  private final EmptyableString host;
  private final EmptyableInteger port;

  private AccountInfoUpdate(final String id,
          final EmptyableString domain,
          final EmptyableString user,
          final EmptyableString password,
          final EmptyableString host,
          final EmptyableInteger port) {
    this.id = id;
    this.domain = domain;
    this.user = user;
    this.password = password;
    this.host = host;
    this.port = port;
  }

  public String getId() {
    return id;
  }

  public EmptyableString getDomain() {
    return domain;
  }

  public EmptyableString getUser() {
    return user;
  }

  public EmptyableString getPassword() {
    return password;
  }

  public EmptyableString getHost() {
    return host;
  }

  public EmptyableInteger getPort() {
    return port;
  }

  public static class Builder extends BaseBuilder {

    private String id = null;
    private EmptyableString domain = null;
    private EmptyableString user = null;
    private EmptyableString password = null;
    private EmptyableString host = null;
    private EmptyableInteger port = null;

    public Builder setId(final String value) {
      if (isValid(value)) {
        id = value;
      }
      return this;
    }

    public Builder setDomain(final EmptyableString value) {
      if (isValid(value)) {
        domain = value;
      }
      return this;
    }

    public Builder setUser(final EmptyableString value) {
      if (isValid(value)) {
        user = value;
      }
      return this;
    }

    public Builder setPassword(final EmptyableString value) {
      if (isValid(value)) {
        password = value;
      }
      return this;
    }

    public Builder setHost(final EmptyableString value) {
      if (isValid(value)) {
        host = value;
      }
      return this;
    }

    public Builder setPort(final EmptyableInteger value) {
      if (isValid(value)) {
        port = value;
      }
      return this;
    }

    public AccountInfoUpdate build() {
      checkNull(id, "id");
      checkNull(domain, "domain");
      checkNull(user, "user");
      checkNull(password, "password");
      checkNull(host, "host");
      checkNull(port, "port");
      return new AccountInfoUpdate(id, domain, user,
              password, host, port);
    }
  }
}
