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
 * ClientSocketCreatorImpl is constructed and held by a TcpSocketCreator. It is
 * accessed through the method {@link TcpSocketCreator#forClient()}.
 */
public class ClientSocketCreatorImpl implements ClientSocketCreator {
  protected final TcpSocketCreatorImpl socketCreator;

  protected ClientSocketCreatorImpl(TcpSocketCreatorImpl socketCreator) {
    this.socketCreator = socketCreator;
  }

  @Override
  public boolean useSSL() {
    return socketCreator.useSSL();
  }

  @Override
  public void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException {
    if (useSSL()) {
      throw new IllegalStateException("Handshake on SSL connections is not supported");
    }
  }

  /**
   * Return a client socket. This method is used by client/server clients.
   */
  public Socket connect(HostAndPort addr, int timeout) throws IOException {
    return socketCreator.connect(addr, timeout, null, true, -1);
  }

  @Override
  public Socket connect(HostAndPort addr, int timeout, int socketBufferSize,
      TcpSocketFactory socketFactory)
      throws IOException {
    return socketCreator.connect(addr, timeout, null, true, socketBufferSize, socketFactory);
  }

}
