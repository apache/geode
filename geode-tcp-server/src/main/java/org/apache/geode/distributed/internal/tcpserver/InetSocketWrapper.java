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
package org.apache.geode.distributed.internal.tcpserver;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;

import org.apache.commons.validator.routines.InetAddressValidator;

class InetSocketWrapper {
  protected InetSocketAddress inetSocketAddress;
  private InetSocketAddress attemptedToResolve;

  protected InetSocketWrapper() {}

  protected InetSocketWrapper(String hostName, int port) {
    if (hostName == null) {
      inetSocketAddress = new InetSocketAddress(port);
    } else if (InetAddressValidator.getInstance().isValid(hostName)) {
      // numeric address - use as-is
      inetSocketAddress = new InetSocketAddress(hostName, port);
    } else {
      // non-numeric address - resolve hostname when needed
      inetSocketAddress = InetSocketAddress.createUnresolved(hostName, port);
    }
  }

  public String getHostName() {
    return inetSocketAddress.getHostName();
  }

  public InetAddress getAddress() {
    return getSocketInetAddress().getAddress();
  }

  @Override
  public int hashCode() {
    return inetSocketAddress.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InetSocketWrapper that = (InetSocketWrapper) o;
    return Objects.equals(inetSocketAddress, that.inetSocketAddress);
  }

  /**
   * Returns an InetSocketAddress for this host and port. An attempt is made to resolve the
   * host name but if resolution fails an unresolved InetSocketAddress is returned. This return
   * value will not hold an InetAddress, so calling getAddress() on it will return null.
   */
  public InetSocketAddress getSocketInetAddress() {
    if (attemptedToResolve == null) {
      attemptedToResolve = basicGetInetSocketAddress();
    }
    return attemptedToResolve;
  }

  private InetSocketAddress basicGetInetSocketAddress() {
    if (inetSocketAddress.isUnresolved()) {
      // note that this leaves the InetAddress null if the hostname isn't resolvable
      return new InetSocketAddress(inetSocketAddress.getHostString(), inetSocketAddress.getPort());
    } else {
      return inetSocketAddress;
    }
  }

  @Override
  public String toString() {
    return inetSocketAddress.toString();
  }

}
