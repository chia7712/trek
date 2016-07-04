package com.spright.trek.datasystem.request;

import java.util.Objects;

public final class DataOwner {

  public static DataOwner newSingleOwner(final String name) {
    return new DataOwner(name, 1.0f);
  }
  private final String hostname;
  private final double ratio;

  public DataOwner(final String host, final double ratio) {
    this.hostname = host;
    this.ratio = ratio;
  }

  public String getHostname() {
    return hostname;
  }

  public double getRatio() {
    return ratio;
  }

  @Override
  public String toString() {
    return hostname + ":" + ratio;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != getClass()) {
      return false;
    }
    DataOwner other = (DataOwner) obj;
    return other.getHostname().equals(other.getHostname())
            && other.getRatio() == getRatio();
  }

  @Override
  public int hashCode() {
    int hash = 3;
    hash = 31 * hash + Objects.hashCode(this.hostname);
    hash = 31 * hash + (int) (Double.doubleToLongBits(this.ratio) ^ (Double.doubleToLongBits(this.ratio) >>> 32));
    return hash;
  }
}
