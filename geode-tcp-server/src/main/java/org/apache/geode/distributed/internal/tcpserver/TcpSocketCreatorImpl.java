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

import java.io.IOException;
import java.net.Socket;


/**
 * TcpSocketCreatorImpl is a simple implementation of TcpSocketCreator for use in the
 * tcp-server and membership subprojects. It does not support SSL - see SocketCreator
 * in geode-core for a more robust and feature-filled TcpSocketCreator implementation.
 */
public class TcpSocketCreatorImpl implements TcpSocketCreator {


  protected ClusterSocketCreatorImpl clusterSocketCreator;
  protected ClientSocketCreatorImpl clientSocketCreator;
  protected AdvancedSocketCreatorImpl advancedSocketCreator;

  public TcpSocketCreatorImpl() {
    initializeCreators();
  }

  protected void initializeCreators() {
    clusterSocketCreator = new ClusterSocketCreatorImpl(this);
    clientSocketCreator = new ClientSocketCreatorImpl(this);
    advancedSocketCreator = new AdvancedSocketCreatorImpl(this);
  }


  protected boolean useSSL() {
    return false;
  }

  Socket connect(HostAndPort addr, int timeout,
      ConnectionWatcher optionalWatcher, boolean clientSide, int socketBufferSize)
      throws IOException {
    return connect(addr, timeout, optionalWatcher, clientSide, socketBufferSize,
        TcpSocketFactory.DEFAULT);
  }

  Socket connect(HostAndPort addr, int timeout,
      ConnectionWatcher optionalWatcher, boolean clientSide, int socketBufferSize,
      TcpSocketFactory proxySocketFactory)
      throws IOException {
    return forAdvancedUse().connect(addr, timeout, optionalWatcher, clientSide, socketBufferSize,
        useSSL(), proxySocketFactory);
  }


  @Override
  public ClusterSocketCreator forCluster() {
    return clusterSocketCreator;
  }

  @Override
  public ClientSocketCreator forClient() {
    return clientSocketCreator;
  }

  @Override
  public AdvancedSocketCreator forAdvancedUse() {
    return advancedSocketCreator;
  }

}
