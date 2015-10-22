/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.org.jgroups.stack;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketImpl;

import com.gemstone.org.jgroups.util.ConnectionWatcher;
import com.gemstone.org.jgroups.util.SockCreator;

public class SockCreatorImpl implements SockCreator {

  @Override
  public boolean useSSL() {
    return false;
  }

  @Override
  public Socket connect(InetAddress ipAddress, int port, int connectTimeout,
      ConnectionWatcher watcher, boolean clientToServer, int bufferSize, boolean useSSL_ignored)
      throws IOException {
    Socket socket = new Socket();
    SocketAddress addr = new InetSocketAddress(ipAddress, port);
    if (connectTimeout > 0) {
      socket.connect(addr, connectTimeout);
    } else {
      socket.connect(addr);
    }
    return socket;
  }

  @Override
  public Socket connect(InetAddress ipAddress, int port, int i,
      ConnectionWatcher watcher, boolean clientToServer) throws IOException {
    Socket socket = new Socket();
    SocketAddress addr = new InetSocketAddress(ipAddress, port);
    socket.connect(addr);
    return socket;
  }

}
