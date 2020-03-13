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

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;

import org.apache.geode.cache.client.SocketFactory;

/**
 * A socket factory that uses a SOCKS5 proxy to connect to locators and servers.
 */
public class SocksSocketFactory implements SocketFactory {

  private Proxy proxy;

  /**
   * Create a socket factory that uses a socks 5 proxy at the given host and port
   *
   * @param hostname The hostname of the proxy
   * @param port The port of the proxy
   */
  public SocksSocketFactory(String hostname, int port) {
    this(new Proxy(Proxy.Type.SOCKS, new InetSocketAddress(
        hostname, port)));
  }

  public SocksSocketFactory(Proxy proxy) {
    this.proxy = proxy;
  }


  @Override
  public Socket createSocket() {
    return new Socket(proxy);
  }
}
