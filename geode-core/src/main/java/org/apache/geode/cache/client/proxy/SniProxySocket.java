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

package org.apache.geode.cache.client.proxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

class SniProxySocket extends Socket {
  private final InetSocketAddress proxyAddress;
  private InetSocketAddress endpoint;

  public SniProxySocket(final InetSocketAddress proxyAddress) {
    this.proxyAddress = proxyAddress;
  }

  @Override
  public void connect(SocketAddress endpoint, int timeout) throws IOException {
    super.connect(proxyAddress, timeout);
    this.endpoint = (InetSocketAddress) endpoint;
  }

  @Override
  public SocketAddress getRemoteSocketAddress() {
    return endpoint;
  }
}
