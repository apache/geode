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


/**
 * HostAddress is a holder of a host name/address. It is the primary
 * way to specify a host address that may or may not have been resolved to an InetAddress.
 *
 * @See HostAndPort which can hold both a host name and a port
 */
public class HostAddress extends InetSocketWrapper {

  public HostAddress(String hostName) {
    super(hostName, 0);
  }

  public HostAddress(InetAddress address) {
    if (address == null) {
      throw new IllegalArgumentException("null parameters are not allowed");
    }
    inetSocketAddress = new InetSocketAddress(address, 0);
  }

  public HostAddress(HostAndPort hostAndPort) {
    hostName = hostAndPort.hostName;
    inetSocketAddress = hostAndPort.inetSocketAddress;
  }

  @Override
  public String toString() {
    if (hostName != null) {
      return hostName;
    } else {
      return inetSocketAddress.getAddress().toString();
    }
  }
}
