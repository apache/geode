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
package org.apache.geode.distributed.internal.membership.api;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;

import org.apache.commons.validator.routines.InetAddressValidator;

import org.apache.geode.distributed.internal.tcpserver.HostAndPort;

/**
 * HostAddress is a holder of a host name/address. It is the primary
 * way to specify a host address that may or may not have been resolved to an InetAddress.
 *
 * @See HostAddress which can hold both a hast name and a port
 */
public class HostAddress {

  private InetSocketAddress address;

  public HostAddress(String hostName) {
    if (hostName == null) {
      address = new InetSocketAddress(0);
    } else if (InetAddressValidator.getInstance().isValid(hostName)) {
      // numeric address - use as-is
      address = new InetSocketAddress(hostName, 0);
    } else {
      // non-numeric address - resolve hostname when needed
      address = InetSocketAddress.createUnresolved(hostName, 0);
    }
  }

  public HostAddress(InetAddress address) {
    if (address == null) {
      throw new IllegalArgumentException("null parameters are not allowed");
    }
    this.address = new InetSocketAddress(address, 0);
  }

  public HostAddress(HostAndPort host) {
    this.address = host.getSocketInetAddress();
  }

  /**
   * Returns an InetSocketAddress for this host and port. An attempt is made to resolve the
   * host name but if resolution fails an unresolved InetSocketAddress is returned. This return
   * value will not hold an InetAddress, so calling getAddress() on it will return null.
   */
  private InetSocketAddress getInetSocketAddress() {
    if (address.isUnresolved()) {
      // note that this leaves the InetAddress null if the hostname isn't resolvable
      return new InetSocketAddress(address.getHostString(), address.getPort());
    } else {
      return this.address;
    }
  }

  public String getHostName() {
    return address.getHostName();
  }

  @Override
  public int hashCode() {
    return address.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HostAddress that = (HostAddress) o;
    return Objects.equals(address, that.address);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "[" + address + "]";
  }

  public InetAddress getAddress() {
    return getInetSocketAddress().getAddress();
  }

}
