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

package org.apache.geode.internal.cache.tier.sockets;

import java.net.InetAddress;
import java.util.Properties;

import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;

public class TcpServerFactory {
  private ClientProtocolMessageHandler protocolHandler;

  public TcpServerFactory() {
    initializeMessageHandler();
  }

  public TcpServer makeTcpServer(int port, InetAddress bind_address, Properties sslConfig,
      DistributionConfigImpl cfg, TcpHandler handler, PoolStatHelper poolHelper,
      ThreadGroup threadGroup, String threadName, InternalLocator internalLocator) {

    return new TcpServer(port, bind_address, sslConfig, cfg, handler, poolHelper, threadGroup,
        threadName, internalLocator, protocolHandler);
  }

  public synchronized ClientProtocolMessageHandler initializeMessageHandler() {
    if (!Boolean.getBoolean("geode.feature-protobuf-protocol")) {
      return null;
    }
    if (protocolHandler != null) {
      return protocolHandler;
    }

    protocolHandler = new MessageHandlerFactory().makeMessageHandler();

    return protocolHandler;
  }
}
