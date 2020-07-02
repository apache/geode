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

package org.apache.geode.cache.client.internal;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ServerQueueStatus;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * A connection source where the list of endpoints is specified explicitly.
 *
 * @since GemFire 5.7
 *
 *        TODO - the UnusedServerMonitor basically will force the pool to have at least one
 *        connection to each server. Maybe we need to have it create connections that are outside
 *        the pool?
 *
 */
public class ExplicitConnectionSourceImpl implements ConnectionSource {

  private static final Logger logger = LogService.getLogger();

  private List<ServerLocation> serverList;
  private int nextServerIndex = 0;
  private int nextQueueIndex = 0;
  private InternalPool pool;

  /**
   * A debug flag, which can be toggled by tests to disable/enable shuffling of the endpoints list
   */
  private boolean DISABLE_SHUFFLING =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "bridge.disableShufflingOfEndpoints");

  ExplicitConnectionSourceImpl(List<InetSocketAddress> contacts) {
    ArrayList<ServerLocation> serverList = new ArrayList<>(contacts.size());
    for (InetSocketAddress addr : contacts) {
      serverList.add(new ServerLocation(addr.getHostString(), addr.getPort()));
    }
    shuffle(serverList);
    this.serverList = Collections.unmodifiableList(serverList);
  }

  @Override
  public synchronized void start(InternalPool pool) {
    this.pool = pool;
    pool.getStats().setInitialContacts(serverList.size());
  }

  @Override
  public void stop() {
    // do nothing
  }

  @Override
  public ServerLocation findReplacementServer(ServerLocation currentServer,
      Set<ServerLocation> excludedServers) {
    // at this time we always try to find a server other than currentServer
    // and if we do return it. Otherwise return null;
    // so that clients would attempt to keep the same number of connections
    // to each server but it would be a bit of work.
    // Plus we need to make sure it would work ok for hardware load balancers.
    HashSet<ServerLocation> excludedPlusCurrent = new HashSet<>(excludedServers);
    excludedPlusCurrent.add(currentServer);
    return findServer(excludedPlusCurrent);
  }

  @Override
  public synchronized ServerLocation findServer(Set excludedServers) {
    if (PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return null;
    }
    ServerLocation nextServer;
    int startIndex = nextServerIndex;
    do {
      nextServer = serverList.get(nextServerIndex);
      if (++nextServerIndex >= serverList.size()) {
        nextServerIndex = 0;
      }
      if (!excludedServers.contains(nextServer)) {
        return nextServer;
      }
    } while (nextServerIndex != startIndex);

    return null;
  }

  /**
   * TODO - this algorithm could be cleaned up. Right now we have to connect to every server in the
   * system to find where our durable queue lives.
   */
  @Override
  public synchronized List<ServerLocation> findServersForQueue(Set<ServerLocation> excludedServers,
      int numServers,
      ClientProxyMembershipID proxyId, boolean findDurableQueue) {
    if (PoolImpl.TEST_DURABLE_IS_NET_DOWN) {
      return new ArrayList<>();
    }
    if (numServers == -1) {
      numServers = Integer.MAX_VALUE;
    }
    if (findDurableQueue && proxyId.isDurable()) {
      return findDurableQueues(excludedServers, numServers);
    } else {
      return pickQueueServers(excludedServers, numServers);
    }
  }

  @Override
  public boolean isBalanced() {
    return false;
  }

  private List<ServerLocation> pickQueueServers(Set<ServerLocation> excludedServers,
      int numServers) {

    ArrayList<ServerLocation> result = new ArrayList<>();
    ServerLocation nextQueue;
    int startIndex = nextQueueIndex;
    do {
      nextQueue = serverList.get(nextQueueIndex);
      if (++nextQueueIndex >= serverList.size()) {
        nextQueueIndex = 0;
      }
      if (!excludedServers.contains(nextQueue)) {
        result.add(nextQueue);
      }
    } while (nextQueueIndex != startIndex && result.size() < numServers);

    return result;
  }

  /**
   * a "fake" operation which just extracts the queue status from the connection
   */
  private static class HasQueueOp implements Op {
    @Immutable
    static final HasQueueOp SINGLETON = new HasQueueOp();

    @Override
    public Object attempt(ClientCacheConnection cnx) throws Exception {
      ServerQueueStatus status = cnx.getQueueStatus();
      return status.isNonRedundant() ? Boolean.FALSE : Boolean.TRUE;
    }
  }

  private List<ServerLocation> findDurableQueues(Set<ServerLocation> excludedServers,
      int numServers) {
    ArrayList<ServerLocation> durableServers = new ArrayList<>();
    ArrayList<ServerLocation> otherServers = new ArrayList<>();

    logger.debug("ExplicitConnectionSource - looking for durable queue");

    for (ServerLocation server : serverList) {
      if (excludedServers.contains(server)) {
        continue;
      }

      // the pool will automatically create a connection to this server
      // and store it for future use.
      Boolean hasQueue;
      try {
        hasQueue = (Boolean) pool.executeOn(server, HasQueueOp.SINGLETON);
      } catch (GemFireSecurityException e) {
        throw e;
      } catch (Exception e) {
        if (e.getCause() instanceof GemFireSecurityException) {
          throw (GemFireSecurityException) e.getCause();
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Unabled to check for durable queue on server {}: {}", server, e);
        }
        continue;
      }
      if (hasQueue != null) {
        if (hasQueue) {
          if (logger.isDebugEnabled()) {
            logger.debug("Durable queue found on {}", server);
          }
          durableServers.add(server);
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("Durable queue was not found on {}", server);
          }
          otherServers.add(server);
        }
      }
    }

    int remainingServers = numServers - durableServers.size();
    if (remainingServers > otherServers.size()) {
      remainingServers = otherServers.size();
    }
    // note, we're always prefering the servers in the beginning of the list
    // but that's ok because we already shuffled the list in our constructor.
    if (remainingServers > 0) {
      durableServers.addAll(otherServers.subList(0, remainingServers));
      nextQueueIndex = remainingServers % serverList.size();
    }

    if (logger.isDebugEnabled()) {
      logger.debug("found {} servers out of {}", durableServers.size(), numServers);
    }

    return durableServers;
  }

  private void shuffle(List endpoints) {
    // this check was copied from ConnectionProxyImpl
    if (endpoints.size() < 2 || DISABLE_SHUFFLING) {
      /*
       * It is not safe to shuffle an ArrayList of size 1 java.lang.IndexOutOfBoundsException:
       * Index: 1, Size: 1 at java.util.ArrayList.RangeCheck(Unknown Source) at
       * java.util.ArrayList.get(Unknown Source) at java.util.Collections.swap(Unknown Source) at
       * java.util.Collections.shuffle(Unknown Source)
       */
      return;
    }
    Collections.shuffle(endpoints);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("EndPoints[");
    synchronized (this) {
      Iterator it = serverList.iterator();
      while (it.hasNext()) {
        ServerLocation loc = (ServerLocation) it.next();
        sb.append(loc.getHostName()).append(":").append(loc.getPort());
        if (it.hasNext()) {
          sb.append(",");
        }
      }
    }
    sb.append("]");
    return sb.toString();
  }

  @Override
  public ArrayList<ServerLocation> getAllServers() {
    return new ArrayList<>(serverList);
  }

  @Override
  public List<InetSocketAddress> getOnlineLocators() {
    return Collections.emptyList();
  }
}
