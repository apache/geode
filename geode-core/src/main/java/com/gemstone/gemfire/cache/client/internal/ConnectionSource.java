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
package com.gemstone.gemfire.cache.client.internal;

import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;


/**
 * A source for discovering servers and finding the least loaded
 * server to connect to.
 * @since GemFire 5.7
 *
 */
public interface ConnectionSource {
  
  ServerLocation findServer(Set/*<ServerLocation>*/ excludedServers);

  /**
   * Asks if we should replace a connection to <code>currentServer</code>
   * with one to the returned server.
   * @param currentServer the server we currently have a connection to.
   * @param excludedServers the replacement server can not be one in this set
   * @return the server we should connect to;
   *         <code>currentServer</code> if a replacement is not needed;
   *         <code>null</code> if no server found
   */
  ServerLocation findReplacementServer(ServerLocation currentServer, Set/*<ServerLocation>*/ excludedServers);
  
  /**
   * Find the servers to host the queue
   * 
   * @param excludedServers
   *                the servers to exclude from the search
   * @param numServers
   *                the number of servers to find, or -1 if we should just find
   *                all of them
   * @param proxyId
   *                the proxy id for this client
   * @param findDurableQueue
   *                if true, the source should make an effort to find the
   *                durable queues for this client
   * @return a list of locations to connect to
   */
  List/* ServerLocation */findServersForQueue(
      Set/* <ServerLocation> */excludedServers, int numServers,
      ClientProxyMembershipID proxyId, boolean findDurableQueue);

  void start(InternalPool poolImpl);

  void stop();
  
  /**
   * Check to see if the load on the servers is balanced, according
   * to this connection source.
   * @return true if the servers have balanced load.
   */
  boolean isBalanced();
}
