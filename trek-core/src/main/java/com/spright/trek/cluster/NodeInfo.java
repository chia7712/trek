package com.spright.trek.cluster;

public class NodeInfo implements Comparable<NodeInfo> {

  private final String address;
  private final int port;

  public NodeInfo(final String address, final int port) {
    if (address == null || address.length() == 0) {
      throw new IllegalArgumentException("The address cannot be null or empty");
    }
    if (port < 0) {
      throw new IllegalArgumentException("The port must be bigger than zero");
    }
    this.address = address;
    this.port = port;
  }

  public String getAddress() {
    return address;
  }

  public int getPort() {
    return port;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null || !obj.getClass().equals(getClass())) {
      return false;
    }
    return compareTo((NodeInfo) obj) == 0;
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public int compareTo(NodeInfo o) {
    int rval = address.compareTo(o.getAddress());
    if (rval == 0) {
      rval = Integer.compare(port, o.getPort());
    }
    return rval;
  }

  @Override
  public String toString() {
    return address + ":" + port;
  }
}
