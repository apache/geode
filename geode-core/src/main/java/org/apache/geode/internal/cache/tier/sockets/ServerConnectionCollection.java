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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

// This is used to form of group of connections for a particular client. Note that these objects are
// managed by the ClientHealthMonitor, which also manages the synchronization of them.
public class ServerConnectionCollection {
  private final Set<ServerConnection> connectionSet = new HashSet<>();

  // Number of connections currently processing messages for this client
  final AtomicInteger connectionsProcessing = new AtomicInteger();

  // Indicates that the server is soon to be or already in the process of terminating connections in
  // this collection.
  volatile boolean isTerminating = false;

  public void addConnection(ServerConnection connection) {
    connectionSet.add(connection);
  }

  public Set<ServerConnection> getConnections() {
    return connectionSet;
  }

  public void removeConnection(ServerConnection connection) {
    connectionSet.remove(connection);
  }

  public boolean incrementConnectionsProcessing() {
    if (!isTerminating) {
      connectionsProcessing.incrementAndGet();

      return true;
    }

    return false;
  }
}
