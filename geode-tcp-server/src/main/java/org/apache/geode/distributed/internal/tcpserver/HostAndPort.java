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

public class HostAndPort {

  private final InetSocketAddress socketInetAddress;

  public HostAndPort(String hostName, int port) {
    if (hostName == null) {
      socketInetAddress = new InetSocketAddress(port);
    } else if (InetAddressValidator.getInstance().isValid(hostName)) {
      // numeric address - use as-is
      socketInetAddress = new InetSocketAddress(hostName, port);
    } else {
      // non-numeric address - resolve hostname when needed
      socketInetAddress = InetSocketAddress.createUnresolved(hostName, port);
    }
  }

  /**
   * If location is not litteral IP address a new resolved {@link InetSocketAddress} is returned.
   *
   * @return resolved {@link InetSocketAddress}, otherwise stored {@link InetSocketAddress} if
   *         literal IP address is used.
   */
  public InetSocketAddress getSocketInetAddress() {
    if (socketInetAddress.isUnresolved()) {
      return new InetSocketAddress(socketInetAddress.getHostString(), socketInetAddress.getPort());
    } else {
      return this.socketInetAddress;
    }
  }

  public String getHostName() {
    return socketInetAddress.getHostString();
  }

  public int getPort() {
    return socketInetAddress.getPort();
  }

  @Override
  public int hashCode() {
    return socketInetAddress.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HostAndPort that = (HostAndPort) o;
    return Objects.equals(socketInetAddress, that.socketInetAddress);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [socketInetAddress=" + socketInetAddress + "]";
  }

  private InetSocketAddress cloneUnresolved(final InetSocketAddress inetSocketAddress) {
    return InetSocketAddress.createUnresolved(inetSocketAddress.getHostString(),
        inetSocketAddress.getPort());
  }

  public InetAddress getAddress() {
    return getSocketInetAddress().getAddress();
  }
}
