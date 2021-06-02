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

import org.apache.commons.validator.routines.InetAddressValidator;

public class LocatorAddress {

  private final InetSocketAddress socketInetAddress;
  private final String hostname;
  private final int port;
  private final boolean isIpString;

  public LocatorAddress(InetSocketAddress loc, String locStr) {
    this.socketInetAddress = loc;
    this.hostname = locStr;
    this.port = loc.getPort();
    this.isIpString = InetAddressValidator.getInstance().isValid(locStr);
  }

  public boolean isIpString() {
    return isIpString;
  }

  /**
   * if host is ipString then it will return the cached InetSocketAddress Otherwise it will create
   * the new instance of InetSocketAddress
   */
  public InetSocketAddress getSocketInetAddress() {
    if (this.isIpString) {
      return this.socketInetAddress;
    } else {
      return new InetSocketAddress(hostname, this.socketInetAddress.getPort());
    }
  }

  public String getHostName() {
    return hostname;
  }

  public int getPort() {
    return port;
  }

  /**
   * If component has retry logic then use this method to get the InetSocketAddress address
   * AutoConnectionSourceImpl for client has retry logic; This way client will not make DNS query
   * each time
   *
   */
  public InetSocketAddress getSocketInetAddressNoLookup() {
    return this.socketInetAddress;
  }

  @Override
  public int hashCode() {
    int prime = 31;
    int result = 1;
    result = prime * result + (isIpString ? 1231 : 1237);
    result = prime * result + (socketInetAddress == null ? 0 : socketInetAddress.hashCode());
    result = prime * result + (hostname == null ? 0 : hostname.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    LocatorAddress other = (LocatorAddress) obj;
    if (isIpString != other.isIpString) {
      return false;
    }
    if (socketInetAddress == null) {
      if (other.socketInetAddress != null) {
        return false;
      }
    } else if (!socketInetAddress.equals(other.socketInetAddress)) {
      return false;
    }
    if (hostname == null) {
      if (other.hostname != null) {
        return false;
      }
    } else if (!hostname.equals(other.hostname)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + " [socketInetAddress=" + socketInetAddress + ", hostname=" + hostname
        + ", isIpString=" + isIpString + "]";
  }

}
