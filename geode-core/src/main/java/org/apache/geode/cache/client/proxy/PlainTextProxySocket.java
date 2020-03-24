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

public class PlainTextProxySocket extends Socket {
  private String proxyHost;
  private InetSocketAddress endpoint;

  public PlainTextProxySocket(String proxyHost) {
    this.proxyHost = proxyHost;
  }

  @Override
  public void connect(SocketAddress endpoint, int timeout) throws IOException {
    this.endpoint = (InetSocketAddress) endpoint;
    super.connect(new InetSocketAddress(proxyHost, this.endpoint.getPort()), timeout);
  }

  @Override
  public SocketAddress getRemoteSocketAddress() {
    return endpoint;
  }
}
