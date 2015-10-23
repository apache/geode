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
package com.gemstone.org.jgroups.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/** This interface does not define how to create socks
 * but defines a factory for creating sockets.
 * One of its implementations had already used
 * the name "SocketCreator" at the time the interface
 * was created.
 */
public interface SockCreator {

  boolean useSSL();

  Socket connect(InetAddress ipAddress, int port, int connectTimeout,
      ConnectionWatcher watcher, boolean clientToServer, int timeout, boolean useSSL) throws IOException;

  Socket connect(InetAddress ipAddress, int port, int timeout,
      ConnectionWatcher watcher, boolean clientToServer
      ) throws IOException;

}
