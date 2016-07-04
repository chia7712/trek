package com.spright.trek.datasystem.request;

import java.util.Objects;
import org.apache.commons.io.FilenameUtils;

public final class DataPath implements Comparable<DataPath> {

  private final String catalog;
  private final String name;

  private static String formatCatalog(final String catalog) {
    final boolean needStartWithSlash = !catalog.startsWith("/");
    final boolean needEndWithSlash = !catalog.endsWith("/");
    if (needStartWithSlash && needEndWithSlash) {
      return "/" + catalog + "/";
    } else if (needStartWithSlash && !needEndWithSlash) {
      return "/" + catalog;
    } else if (!needStartWithSlash && needEndWithSlash) {
      return catalog + "/";
    } else {
      return catalog;
    }
  }

  public DataPath(final String catalog, final String name) {
    this.catalog = formatCatalog(catalog);
    this.name = name;
    if (catalog == null || catalog.length() == 0
            || name == null || name.length() == 0) {
      throw new NullPointerException("The catalog and name cannot be null or empty");
    }
  }

  public DataPath(final String fullPath) {
    this(formatCatalog(FilenameUtils.getFullPath(fullPath)),
            FilenameUtils.getName(fullPath));
  }

  public DataPath append(final String name) {
    return new DataPath(toString(), name);
  }

  public String getCatalog() {
    return catalog;
  }

  public String getName() {
    return name;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    DataPath path = (DataPath) obj;
    return toString().equals(path.toString());
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 13 * hash + Objects.hashCode(this.catalog);
    hash = 13 * hash + Objects.hashCode(this.name);
    return hash;
  }

  @Override
  public String toString() {
    return catalog + name;
  }

  @Override
  public int compareTo(DataPath o) {
    return toString().compareTo(o.toString());
  }
}
