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

import java.net.InetSocketAddress;
import java.util.Objects;

import org.apache.commons.validator.routines.InetAddressValidator;

public class LocatorAddress {

  private final InetSocketAddress socketInetAddress;

  public LocatorAddress(InetSocketAddress loc, String locStr) {
    if (InetAddressValidator.getInstance().isValid(locStr)) {
      socketInetAddress = new InetSocketAddress(locStr, loc.getPort());
    } else {
      socketInetAddress = cloneUnresolved(loc);
    }
  }

  /**
   * @deprecated Users should not care if literal IP or hostname is used.
   */
  @Deprecated
  public boolean isIpString() {
    return !socketInetAddress.isUnresolved();
  }

  /**
   * If location is not litteral IP address a new unresolved {@link InetSocketAddress} is returned.
   *
   * @return unresolved {@link InetSocketAddress}, otherwise resolved {@link InetSocketAddress} if
   *         literal IP address is used.
   */
  public InetSocketAddress getSocketInetAddress() {
    if (socketInetAddress.isUnresolved()) {
      return cloneUnresolved(socketInetAddress);
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

  /**
   * If component has retry logic then use this method to get the InetSocketAddress address
   * AutoConnectionSourceImpl for client has retry logic; This way client will not make DNS query
   * each time
   *
   * @deprecated Use {@link #getSocketInetAddress()}
   */
  @Deprecated
  public InetSocketAddress getSocketInetAddressNoLookup() {
    return getSocketInetAddress();
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
    LocatorAddress that = (LocatorAddress) o;
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

}
