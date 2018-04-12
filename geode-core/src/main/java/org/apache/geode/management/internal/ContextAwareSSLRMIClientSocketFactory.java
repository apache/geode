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

package org.apache.geode.management.internal;

import java.io.IOException;
import java.io.Serializable;
import java.net.Socket;
import java.rmi.server.RMIClientSocketFactory;

import javax.rmi.ssl.SslRMIClientSocketFactory;

import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

/**
 * What's going on here? For jar deployment we're using the RMI-IO library which uses RMI to
 * allow streaming files. When using SSL with RMI, the weird thing is that the
 * RMIClientSocketFactory is defined on the server where the relevant object is being exported.
 * This factory is serialized to the client to be used when making calls back to the server. Thus
 * the factory needs to be able to create sockets configured according to the remote configuration.
 */
public class ContextAwareSSLRMIClientSocketFactory implements RMIClientSocketFactory, Serializable {

  private static final long serialVersionUID = 8159615071011918570L;

  private static final SslRMIClientSocketFactory defaultFactory = new SslRMIClientSocketFactory();

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    SocketCreator socketCreator;
    try {
      socketCreator =
          SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX);
    } catch (Exception exception) {
      /*
       * In the context of a gfsh VM the javax.net.ssl properties are used to configure SSL
       * appropriately - see the constructor for JMXOperationInvoker.
       */
      return defaultFactory.createSocket(host, port);
    }

    return socketCreator.connectForClient(host, port, 0);
  }

}
