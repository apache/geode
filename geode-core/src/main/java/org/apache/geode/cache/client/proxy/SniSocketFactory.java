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
import java.util.Properties;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.client.SocketFactory;
import org.apache.geode.internal.DistributionLocator;

/**
 * A {@link SocketFactory} that connects a client to locators and servers
 * through a SNI proxy.
 */
public class SniSocketFactory implements SocketFactory, Declarable {


  private String hostname;
  private int port;

  public SniSocketFactory() {} // required by Declarable

  public SniSocketFactory(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override // Declarable
  public void initialize(Cache cache, Properties properties) {
    this.hostname = properties.getProperty("hostname");
    String portString =
        properties.getProperty("port", "" + DistributionLocator.DEFAULT_LOCATOR_PORT);
    this.port = Integer.parseInt(portString);
  }

  @Override
  public Socket createSocket() throws IOException {
    return new SniProxySocket(new InetSocketAddress(hostname, port));
  }

  public String getHostname() {
    return hostname;
  }

  public int getPort() {
    return port;
  }
}
