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
package org.apache.geode.internal.net;

import java.io.IOException;
import java.net.Socket;

import org.apache.geode.distributed.internal.tcpserver.ClientSocketCreatorImpl;

class SCClientSocketCreator extends ClientSocketCreatorImpl {
  private final SocketCreator coreSocketCreator;

  protected SCClientSocketCreator(SocketCreator socketCreator) {
    super(socketCreator);
    coreSocketCreator = socketCreator;
  }

  @Override
  public void handshakeIfSocketIsSSL(Socket socket, int timeout) throws IOException {
    coreSocketCreator.handshakeIfSocketIsSSL(socket, timeout);
  }

}
