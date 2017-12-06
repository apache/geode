/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package java.net;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Replace the JDK's InetAddress with a mock implementation that uses no native calls or IO.
 *
 * This implementation is simply a holder for a hostname, no DNS lookups are done. The address
 * returned from getAddress is simply the hostname converted to bytes in UTF-8
 */
public class InetAddress implements java.io.Serializable {

  public String hostname;

  public InetAddress(String host) {
    this.hostname = host;
  }

  public boolean isMulticastAddress() {
    return false;
  }

  public boolean isAnyLocalAddress() {
    return false;
  }

  public boolean isLoopbackAddress() {
    return false;
  }

  public boolean isLinkLocalAddress() {
    return false;
  }

  public boolean isSiteLocalAddress() {
    return false;
  }

  public boolean isMCGlobal() {
    return false;
  }

  public boolean isMCNodeLocal() {
    return false;
  }

  public boolean isMCLinkLocal() {
    return false;
  }

  public boolean isMCSiteLocal() {
    return false;
  }

  public boolean isMCOrgLocal() {
    return false;
  }


  public boolean isReachable(int timeout) throws IOException {
    return false;
  }

  public boolean isReachable(NetworkInterface netif, int ttl, int timeout) throws IOException {
    return false;
  }

  public String getHostName() {
    return hostname;
  }

  public String getCanonicalHostName() {
    return hostname;
  }


  public byte[] getAddress() {
    return hostname.getBytes(Charset.forName("UTF-8"));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    InetAddress that = (InetAddress) o;

    return hostname != null ? hostname.equals(that.hostname) : that.hostname == null;
  }

  @Override
  public int hashCode() {
    return hostname != null ? hostname.hashCode() : 0;
  }

  /**
   * Converts this IP address to a {@code String}. The string returned is of the form: hostname /
   * literal IP address.
   *
   * If the host name is unresolved, no reverse name service lookup is performed. The hostname part
   * will be represented by an empty string.
   *
   * @return a string representation of this IP address.
   */
  public String toString() {
    return hostname;
  }


  public static InetAddress getByAddress(String host, byte[] addr) throws UnknownHostException {
    return new InetAddress(host);
  }


  public static InetAddress getByName(String host) throws UnknownHostException {
    return new InetAddress(host);
  }

  public static InetAddress[] getAllByName(String host) throws UnknownHostException {
    return new InetAddress[] {new InetAddress("localhost")};
  }

  public static InetAddress getLoopbackAddress() {
    return new InetAddress("localhost");
  }


  public static InetAddress getByAddress(byte[] addr) throws UnknownHostException {
    String host = new String(addr, Charset.forName("UTF-8"));
    return getByName(host);
  }

  public static InetAddress getLocalHost() throws UnknownHostException {
    return getLoopbackAddress();
  }
}
