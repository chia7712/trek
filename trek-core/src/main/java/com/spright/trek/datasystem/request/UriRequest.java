package com.spright.trek.datasystem.request;

import com.spright.trek.datasystem.*;
import com.spright.trek.exception.MappingIOException;
import com.spright.trek.exception.UriParseIOException;
import com.spright.trek.mapping.AccountInfo;
import java.util.stream.Stream;
import com.spright.trek.mapping.Mapping;
import com.spright.trek.DConstants;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class UriRequest {

  private static final Log LOG = LogFactory.getLog(UriRequest.class);

  public static UriRequest parseTo(final Map<String, String> rawQuery,
          final Mapping mapping, final String alternativeName)
          throws UriParseIOException, MappingIOException {
    String uriString = rawQuery.get(DConstants.URI_DATA_TO);
    if (uriString == null) {
      throw new UriParseIOException("No found of \""
              + DConstants.URI_DATA_TO + "\" query");
    }
    return parse(uriString, mapping, alternativeName);
  }

  public static UriRequest parseFrom(final Map<String, String> rawQuery,
          final Mapping mapping, final String alternativeName)
          throws UriParseIOException, MappingIOException {
    String uriString = rawQuery.get(DConstants.URI_DATA_FROM);
    if (uriString == null) {
      throw new UriParseIOException("No found of \""
              + DConstants.URI_DATA_FROM + "\" query");
    }
    return parse(uriString, mapping, alternativeName);
  }

  public static UriRequest parse(final String uriString,
          final Mapping mapping, final String alternativeName) throws UriParseIOException, MappingIOException {
    final String schemeEndString = "://";
    final int schemeEndIndex = uriString.indexOf(schemeEndString);
    if (schemeEndIndex == -1 || schemeEndIndex == 0) {
      throw new UriParseIOException("Invalid format of scheme on " + uriString);
    }
    final String scheme = uriString.substring(0, schemeEndIndex);
    final Protocol protocol = Stream.of(Protocol.values())
            .filter(p -> p.name().equalsIgnoreCase(scheme))
            .findFirst()
            .orElseThrow(() -> new UriParseIOException("No found supported scheme:" + scheme));

    final int accountStartIndex = schemeEndIndex + schemeEndString.length();
    final String accountEndString = "/";
    final int accountEndIndex = uriString.indexOf(accountEndString, accountStartIndex);
    if (accountEndIndex == -1) {
      throw new UriParseIOException("No found of path prefix on " + uriString);
    }
    AccountInfo account = AccountInfo.EMPTY;
    if (accountEndIndex != accountStartIndex) {
      final String accountString = uriString.substring(accountStartIndex, accountEndIndex);
      if (mapping != null && accountString.startsWith("$")) {
        final String id = accountString.substring(1);
        if (id.length() > 0) {
          account = mapping.find(id)
                  .orElseThrow(() -> new UriParseIOException(
                          "No corresponding account for alias on "
                          + uriString));
        }
      } else {
        final int hasAccountIndex = accountString.indexOf("@");
        Map<Integer, String> splits = new TreeMap<>();
        final int domainOrder = 0;
        final int userOrder = 1;
        final int pwdOrder = 2;
        final int hostOrder = 3;
        final int portOrder = 4;
        final String hostAndPort = (hasAccountIndex == -1) ? accountString
                : accountString.substring(hasAccountIndex + 1, accountString.length());
        final int portIndex = hostAndPort.indexOf(":");
        if (portIndex == -1) {
          splits.put(hostOrder, hostAndPort);
          splits.put(portOrder, "");
        } else {
          splits.put(hostOrder, hostAndPort.substring(0, portIndex));
          splits.put(portOrder, hostAndPort.substring(portIndex + 1, hostAndPort.length()));
        }
        if (hasAccountIndex != -1) {
          String domainAndUserAndPassword = accountString.substring(0, hasAccountIndex);
          final int pwdIndex = domainAndUserAndPassword.lastIndexOf(":");
          if (pwdIndex != -1) {
            splits.put(pwdOrder, domainAndUserAndPassword.substring(pwdIndex + 1, domainAndUserAndPassword.length()));
            domainAndUserAndPassword = domainAndUserAndPassword.substring(0, pwdIndex);
          } else {
            splits.put(pwdIndex, "");
          }
          final int domainIndex = domainAndUserAndPassword.lastIndexOf(";");
          if (domainIndex != -1) {
            splits.put(domainOrder, domainAndUserAndPassword.substring(0, domainIndex));
            splits.put(userOrder, domainAndUserAndPassword.substring(domainIndex + 1, domainAndUserAndPassword.length()));
          } else {
            splits.put(domainOrder, "");
            splits.put(userOrder, domainAndUserAndPassword);
          }
        } else {
          splits.put(domainOrder, "");
          splits.put(userOrder, "");
          splits.put(pwdOrder, "");
        }

        if (splits.size() != 5) {
          throw new RuntimeException("The splits from account should be "
                  + 5 + ", current value is " + splits.size());
        }
        try {
          account = AccountInfo.newBuilder()
                  .setDomain(splits.get(domainOrder))
                  .setUser(splits.get(userOrder))
                  .setEncodedPassword(splits.get(pwdOrder))
                  .setHost(splits.get(hostOrder))
                  .setPort(splits.get(portOrder).length() == 0 ? -1 : Integer.valueOf(splits.get(portOrder)))
                  .build();
        } catch (NumberFormatException e) {
          LOG.error("Invalid port number:" + splits.get(portOrder));
          throw new UriParseIOException(e);
        }
      }
    }
    final int pathStartIndex = accountEndIndex;
    final String pathEndString = "?";
    final int pathEndIndex = uriString.indexOf(pathEndString, pathStartIndex);
    String pathString;
    if (pathEndIndex == -1) {
      pathString = uriString.substring(pathStartIndex, uriString.length());
    } else {
      pathString = uriString.substring(pathStartIndex, pathEndIndex);
    }
    final int nameStartIndex = pathString.lastIndexOf("/");
    if (nameStartIndex == -1) {
      throw new UriParseIOException("Invalid forma of path on "
              + uriString);
    }
    if ((nameStartIndex + 1) == pathString.length()
            && alternativeName != null) {
      pathString = pathString + alternativeName;
    }
    return new UriRequest(protocol, account, new DataPath(pathString));
  }
  private final Protocol scheme;
  private final AccountInfo account;
  private final DataPath path;
  private Object attach = null;

  public UriRequest(final Protocol scheme, final AccountInfo account,
          final DataPath path) {
    Objects.requireNonNull(scheme, "The scheme cannot be null");
    Objects.requireNonNull(account, "The account cannot be null");
    Objects.requireNonNull(path, "The path cannot be null");
    this.scheme = scheme;
    this.account = account;
    this.path = path;
  }

  public UriRequest(final UriRequest ref) {
    scheme = ref.getScheme();
    account = ref.getAccountInfo();
    path = ref.getPath();
  }

  public void attach(final Object obj) {
    if (attach != null) {
      throw new RuntimeException("Invalid to attach object repeatedly");
    }
    attach = obj;
  }

  public UriRequest appendName(final String name) {
    return new UriRequest(scheme, account, path.append(name));
  }

  public boolean hasAttach() {
    return attach != null;
  }

  public Optional<Object> getAttach() {
    return Optional.ofNullable(attach);
  }

  public final Protocol getScheme() {
    return scheme;
  }

  public final AccountInfo getAccountInfo() {
    return account;
  }

  public DataPath getPath() {
    return path;
  }

  public String toString(final boolean useId) {
    return new StringBuilder()
            .append(scheme.name().toLowerCase())
            .append("://")
            .append(account.toString(useId))
            .append(path.toString())
            .toString();
  }

  @Override
  public String toString() {
    return toString(true);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    UriRequest other = (UriRequest) obj;
    return other.getScheme() == getScheme()
            && other.getPath().equals(getPath())
            && other.getAccountInfo().equals(getAccountInfo());
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 73 * hash + Objects.hashCode(this.scheme);
    hash = 73 * hash + Objects.hashCode(this.account);
    hash = 73 * hash + Objects.hashCode(this.path);
    return hash;
  }
}
