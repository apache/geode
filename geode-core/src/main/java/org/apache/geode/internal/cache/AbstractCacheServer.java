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
package org.apache.geode.internal.cache;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.server.ClientSubscriptionConfig;
import org.apache.geode.cache.server.ServerLoadProbe;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.ClientMembershipMessage;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.management.membership.ClientMembership;
import org.apache.geode.management.membership.ClientMembershipEvent;
import org.apache.geode.management.membership.ClientMembershipListener;

/**
 * Abstract class that contains common code that all true implementations of {@link CacheServer} can
 * use.
 *
 * @since GemFire 5.7
 */
public abstract class AbstractCacheServer implements InternalCacheServer {

  public static final String TEST_OVERRIDE_DEFAULT_PORT_PROPERTY =
      DistributionConfig.GEMFIRE_PREFIX + "test.CacheServer.OVERRIDE_DEFAULT_PORT";

  /** The cache that is served by this cache server */
  protected final InternalCache cache;

  /** The port that the cache server was configured to run on */
  protected int port;

  /** The maximum number of connections that the BridgeServer will accept */
  protected int maxConnections;

  /** The maximum number of threads that the BridgeServer will create */
  protected int maxThreads;

  /** Whether the cache server notifies by subscription */
  protected boolean notifyBySubscription = true;

  /**
   * The buffer size in bytes of the socket for this <code>BridgeServer</code>
   */
  protected int socketBufferSize;

  /**
   * The tcpNoDelay setting for outgoing sockets
   */
  protected boolean tcpNoDelay;

  /**
   * The maximum amount of time between client pings. This value is used by the
   * <code>ClientHealthMonitor</code> to determine the health of this <code>BridgeServer</code>'s
   * clients.
   */
  protected int maximumTimeBetweenPings;

  /** the maximum number of messages that can be enqueued in a client-queue. */
  protected int maximumMessageCount;

  /**
   * the time (in seconds) after which a message in the client queue will expire.
   */
  protected int messageTimeToLive;
  /**
   * The groups this server belongs to. Use <code>getGroups</code> to read.
   *
   * @since GemFire 5.7
   */
  protected String[] groups;

  protected ServerLoadProbe loadProbe;

  /**
   * The ip address or host name that this server is to listen on.
   *
   * @since GemFire 5.7
   */
  protected String bindAddress;
  /**
   * The ip address or host name that will be given to clients so they can connect to this server
   *
   * @since GemFire 5.7
   */
  protected String hostnameForClients;

  /**
   * How frequency to poll the load on this server.
   */
  protected long loadPollInterval;

  protected ClientSubscriptionConfig clientSubscriptionConfig;

  /**
   * Listens to client membership events and notifies any admin members as clients of this server
   * leave/crash.
   */
  protected final ClientMembershipListener listener;

  ////////////////////// Constructors //////////////////////

  /**
   * Creates a new <code>BridgeServer</code> with the default configuration.
   *
   * @param cache The cache being served
   */
  public AbstractCacheServer(InternalCache cache) {
    this(cache, true);
  }

  public AbstractCacheServer(InternalCache cache, boolean attachListener) {
    this.cache = cache;
    this.port = Integer.getInteger(TEST_OVERRIDE_DEFAULT_PORT_PROPERTY, CacheServer.DEFAULT_PORT);
    this.maxConnections = CacheServer.DEFAULT_MAX_CONNECTIONS;
    this.maxThreads = CacheServer.DEFAULT_MAX_THREADS;
    this.socketBufferSize = CacheServer.DEFAULT_SOCKET_BUFFER_SIZE;
    this.tcpNoDelay = CacheServer.DEFAULT_TCP_NO_DELAY;
    this.maximumTimeBetweenPings = CacheServer.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS;
    this.maximumMessageCount = CacheServer.DEFAULT_MAXIMUM_MESSAGE_COUNT;
    this.messageTimeToLive = CacheServer.DEFAULT_MESSAGE_TIME_TO_LIVE;
    this.groups = CacheServer.DEFAULT_GROUPS;
    this.bindAddress = CacheServer.DEFAULT_BIND_ADDRESS;
    this.hostnameForClients = CacheServer.DEFAULT_HOSTNAME_FOR_CLIENTS;
    this.loadProbe = CacheServer.DEFAULT_LOAD_PROBE;
    this.loadPollInterval = CacheServer.DEFAULT_LOAD_POLL_INTERVAL;
    this.clientSubscriptionConfig = new ClientSubscriptionConfigImpl();

    if (!attachListener) {
      this.listener = null;
      return;
    }
    listener = new ClientMembershipListener() {

      @Override
      public void memberJoined(ClientMembershipEvent event) {
        if (event.isClient()) {
          createAndSendMessage(event, ClientMembershipMessage.JOINED);
        }
      }

      @Override
      public void memberLeft(ClientMembershipEvent event) {
        if (event.isClient()) {
          createAndSendMessage(event, ClientMembershipMessage.LEFT);
        }
      }

      @Override
      public void memberCrashed(ClientMembershipEvent event) {
        if (event.isClient()) {
          createAndSendMessage(event, ClientMembershipMessage.CRASHED);
        }
      }

      /**
       * Method to create & send the ClientMembershipMessage to admin members. The message is sent
       * only if there are any admin members in the distribution system.
       *
       * @param event describes a change in client membership
       * @param type type of event - one of ClientMembershipMessage.JOINED,
       *        ClientMembershipMessage.LEFT, ClientMembershipMessage.CRASHED
       */
      private void createAndSendMessage(ClientMembershipEvent event, int type) {
        InternalDistributedSystem ds = null;
        Cache cacheInstance = AbstractCacheServer.this.cache;
        if (cacheInstance != null && !(cacheInstance instanceof CacheCreation)) {
          ds = (InternalDistributedSystem) cacheInstance.getDistributedSystem();
        } else {
          ds = InternalDistributedSystem.getAnyInstance();
        }

        // ds could be null
        if (ds != null && ds.isConnected()) {
          DistributionManager dm = ds.getDistributionManager();
          Set adminMemberSet = dm.getAdminMemberSet();

          /* check if there are any admin members at all */
          if (!adminMemberSet.isEmpty()) {
            DistributedMember member = event.getMember();

            ClientMembershipMessage msg = new ClientMembershipMessage(event.getMemberId(),
                member == null ? null : member.getHost(), type);

            msg.setRecipients(adminMemberSet);
            dm.putOutgoing(msg);
          }
        }
      }
    };

    ClientMembership.registerClientMembershipListener(listener);
  }

  ///////////////////// Instance Methods /////////////////////

  public int getPort() {
    return this.port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getBindAddress() {
    return this.bindAddress;
  }

  public void setBindAddress(String address) {
    this.bindAddress = address;
  }

  public String getHostnameForClients() {
    return this.hostnameForClients;
  }

  public void setHostnameForClients(String name) {
    this.hostnameForClients = name;
  }

  public int getMaxConnections() {
    return this.maxConnections;
  }

  public void setMaxConnections(int maxCon) {
    this.maxConnections = maxCon;
  }

  public int getMaxThreads() {
    return this.maxThreads;
  }

  public void setMaxThreads(int maxThreads) {
    this.maxThreads = maxThreads;
  }

  public void start() throws IOException {
    // This method is invoked during testing, but it is not necessary
    // to do anything.
  }

  public void setNotifyBySubscription(boolean b) {
    // this.notifyBySubscription = true;
  }

  public boolean getNotifyBySubscription() {
    return this.notifyBySubscription;
  }

  public void setSocketBufferSize(int socketBufferSize) {
    this.socketBufferSize = socketBufferSize;
  }

  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }

  public void setMaximumTimeBetweenPings(int maximumTimeBetweenPings) {
    this.maximumTimeBetweenPings = maximumTimeBetweenPings;
  }

  public int getMaximumTimeBetweenPings() {
    return this.maximumTimeBetweenPings;
  }

  public int getMaximumMessageCount() {
    return this.maximumMessageCount;
  }

  public void setMaximumMessageCount(int maximumMessageCount) {
    this.maximumMessageCount = maximumMessageCount;
  }

  public int getMessageTimeToLive() {
    return this.messageTimeToLive;
  }

  public void setMessageTimeToLive(int messageTimeToLive) {
    this.messageTimeToLive = messageTimeToLive;
  }

  public void setGroups(String[] groups) {
    if (groups == null) {
      this.groups = CacheServer.DEFAULT_GROUPS;
    } else if (groups.length > 0) {
      // copy it for isolation
      String[] copy = new String[groups.length];
      System.arraycopy(groups, 0, copy, 0, groups.length);
      this.groups = copy;
    } else {
      this.groups = CacheServer.DEFAULT_GROUPS; // keep findbugs happy
    }
  }

  public String[] getGroups() {
    String[] result = this.groups;
    if (result.length > 0) {
      // copy it for isolation
      String[] copy = new String[result.length];
      System.arraycopy(result, 0, copy, 0, result.length);
      result = copy;
    }
    return result;
  }

  public ServerLoadProbe getLoadProbe() {
    return loadProbe;
  }

  public void setLoadProbe(ServerLoadProbe loadProbe) {
    this.loadProbe = loadProbe;
  }

  public long getLoadPollInterval() {
    return loadPollInterval;
  }

  public void setLoadPollInterval(long loadPollInterval) {
    this.loadPollInterval = loadPollInterval;
  }

  public void setTcpNoDelay(boolean setting) {
    this.tcpNoDelay = setting;
  }

  public boolean getTcpNoDelay() {
    return this.tcpNoDelay;
  }

  public Cache getCache() {
    return this.cache;
  }

  private static boolean eq(String s1, String s2) {
    if (s1 == null) {
      return s2 == null;
    } else {
      return s1.equals(s2);
    }
  }

  /**
   * Returns whether or not this cache server has the same configuration as another cache server.
   */
  public boolean sameAs(CacheServer other) {
    return getPort() == other.getPort() && eq(getBindAddress(), other.getBindAddress())
        && getSocketBufferSize() == other.getSocketBufferSize()
        && getMaximumTimeBetweenPings() == other.getMaximumTimeBetweenPings()
        && getNotifyBySubscription() == other.getNotifyBySubscription()
        && getMaxConnections() == other.getMaxConnections()
        && getMaxThreads() == other.getMaxThreads()
        && getMaximumMessageCount() == other.getMaximumMessageCount()
        && getMessageTimeToLive() == other.getMessageTimeToLive()
        && Arrays.equals(getGroups(), other.getGroups())
        && getLoadProbe().equals(other.getLoadProbe())
        && getLoadPollInterval() == other.getLoadPollInterval()
        && getTcpNoDelay() == other.getTcpNoDelay();
  }
}
