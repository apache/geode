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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.SocketFactory;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallback;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * Implementation of PoolFactory.
 *
 * @since GemFire 5.7
 */
public class PoolFactoryImpl implements InternalPoolFactory {
  private static final Logger logger = LogService.getLogger();

  /**
   * Used internally to pass the attributes from this factory to the real pool it is creating.
   */
  private PoolAttributes attributes = new PoolAttributes();

  private final List<HostAndPort> locatorAddresses = new ArrayList<>();

  /**
   * The cache that created this factory
   */
  private final PoolManagerImpl pm;

  public PoolFactoryImpl(PoolManagerImpl pm) {
    this.pm = pm;
  }

  @Override
  public PoolFactory setSocketConnectTimeout(int socketConnectTimeout) {
    if (socketConnectTimeout <= -1) {
      throw new IllegalArgumentException("socketConnectTimeout must be greater than -1");
    }
    attributes.socketConnectTimeout = socketConnectTimeout;
    return this;
  }

  @Override
  public PoolFactory setFreeConnectionTimeout(int connectionTimeout) {
    if (connectionTimeout <= 0) {
      throw new IllegalArgumentException("connectionTimeout must be greater than zero");
    }
    attributes.connectionTimeout = connectionTimeout;
    return this;
  }

  @Override
  public PoolFactory setServerConnectionTimeout(int serverConnectionTimeout) {
    if (serverConnectionTimeout < 0) {
      throw new IllegalArgumentException(
          "serverConnectionTimeout must be greater than or equal to 0");
    }
    attributes.serverConnectionTimeout = serverConnectionTimeout;
    return this;
  }

  @Override
  public PoolFactory setLoadConditioningInterval(int connectionLifetime) {
    if (connectionLifetime < -1) {
      throw new IllegalArgumentException("connectionLifetime must be greater than or equal to -1");
    }
    attributes.connectionLifetime = connectionLifetime;
    return this;
  }

  @Override
  public PoolFactory setSocketBufferSize(int bufferSize) {
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("socketBufferSize must be greater than zero");
    }
    attributes.socketBufferSize = bufferSize;
    return this;
  }

  @Override
  @Deprecated
  public PoolFactory setThreadLocalConnections(boolean threadLocalConnections) {
    logger.warn("Use of PoolFactory.setThreadLocalConnections is deprecated and ignored.");
    attributes.threadLocalConnections = threadLocalConnections;
    return this;
  }

  @Override
  public PoolFactory setIdleTimeout(long idleTimout) {
    if (idleTimout < -1) {
      throw new IllegalArgumentException("idleTimeout must be greater than or equal to -1");
    }
    attributes.idleTimeout = idleTimout;
    return this;
  }

  @Override
  public PoolFactory setMaxConnections(int maxConnections) {
    if (maxConnections < attributes.minConnections && maxConnections != -1) {
      throw new IllegalArgumentException(
          "maxConnections must be greater than or equal to minConnections ("
              + attributes.minConnections + ")");
    }
    if (maxConnections <= 0 && maxConnections != -1) {
      throw new IllegalArgumentException(
          "maxConnections must be greater than 0, or set to -1 (no max)");
    }
    attributes.maxConnections = maxConnections;
    return this;
  }

  @Override
  public PoolFactory setMinConnections(int minConnections) {
    if (minConnections > attributes.maxConnections && attributes.maxConnections != -1) {
      throw new IllegalArgumentException(
          "must be less than or equal to maxConnections (" + attributes.maxConnections + ")");
    }
    if (minConnections < 0) {
      throw new IllegalArgumentException("must be greater than or equal to 0");
    }
    attributes.minConnections = minConnections;
    return this;
  }

  @Override
  public PoolFactory setPingInterval(long pingInterval) {
    if (pingInterval <= 0) {
      throw new IllegalArgumentException("pingInterval must be greater than zero");
    }
    attributes.pingInterval = pingInterval;
    return this;
  }

  @Override
  public PoolFactory setStatisticInterval(int statisticInterval) {
    if (statisticInterval < -1) {
      throw new IllegalArgumentException("statisticInterval must be greater than or equal to -1");
    }
    attributes.statisticInterval = statisticInterval;
    return this;
  }

  @Override
  public PoolFactory setRetryAttempts(int retryAttempts) {
    if (retryAttempts < -1) {
      throw new IllegalArgumentException("retryAttempts must be greater than or equal to -1");
    }
    attributes.retryAttempts = retryAttempts;
    return this;
  }

  @Override
  public PoolFactory setReadTimeout(int timeout) {
    if (timeout < 0) {
      throw new IllegalArgumentException("readTimeout must be greater than or equal to zero");
    }
    attributes.readTimeout = timeout;
    return this;
  }

  @Override
  public PoolFactory setServerGroup(String group) {
    if (group == null) {
      group = DEFAULT_SERVER_GROUP;
    }
    attributes.serverGroup = group;
    return this;
  }

  @Override
  public PoolFactory setSubscriptionEnabled(boolean enabled) {
    attributes.queueEnabled = enabled;
    return this;
  }

  @Override
  public PoolFactory setPRSingleHopEnabled(boolean enabled) {
    attributes.prSingleHopEnabled = enabled;
    return this;
  }

  @Override
  public PoolFactory setMultiuserAuthentication(boolean enabled) {
    attributes.multiuserSecureModeEnabled = enabled;
    return this;
  }

  @Override
  public PoolFactory setSocketFactory(SocketFactory socketFactory) {
    attributes.socketFactory = socketFactory;
    return this;
  }

  public PoolFactory setStartDisabled(boolean disable) {
    attributes.startDisabled = disable;
    return this;
  }

  public PoolFactory setLocatorDiscoveryCallback(LocatorDiscoveryCallback callback) {
    attributes.locatorCallback = callback;
    return this;
  }

  @Override
  public PoolFactory setSubscriptionRedundancy(int redundancyLevel) {
    if (redundancyLevel < -1) {
      throw new IllegalArgumentException(
          "queueRedundancyLevel must be greater than or equal to -1");
    }
    attributes.queueRedundancyLevel = redundancyLevel;
    return this;
  }

  @Override
  public PoolFactory setSubscriptionMessageTrackingTimeout(int messageTrackingTimeout) {
    if (messageTrackingTimeout <= 0) {
      throw new IllegalArgumentException("queueMessageTrackingTimeout must be greater than zero");
    }
    attributes.queueMessageTrackingTimeout = messageTrackingTimeout;
    return this;
  }

  @Override
  public PoolFactory setSubscriptionTimeoutMultiplier(int multiplier) {
    attributes.subscriptionTimeoutMultipler = multiplier;
    return this;
  }

  @Override
  public PoolFactory setSubscriptionAckInterval(int ackInterval) {
    if (ackInterval <= 0) {
      throw new IllegalArgumentException("ackInterval must be greater than 0");
    }
    attributes.queueAckInterval = ackInterval;

    return this;
  }

  @Override
  public PoolFactory addLocator(String host, int port) {
    if (attributes.servers.size() > 0) {
      throw new IllegalStateException(
          "A server has already been added. You can only add locators or servers; not both.");
    }
    validatePort(port);
    HostAndPort address = new HostAndPort(host, port);
    attributes.locators.add(address);
    locatorAddresses.add(address);
    return this;
  }

  @Override
  public PoolFactory addServer(String host, int port) {
    if (attributes.locators.size() > 0) {
      throw new IllegalStateException(
          "A locator has already been added. You can only add locators or servers; not both.");
    }
    validatePort(port);
    attributes.servers.add(new HostAndPort(host, port));
    return this;
  }

  private void validatePort(int port) {
    if (port <= 0) {
      throw new IllegalArgumentException("port must be greater than 0 but was " + port);
    }
  }

  @Override
  public PoolFactory reset() {
    // preserve the startDisabled across resets
    boolean sd = attributes.startDisabled;
    attributes = new PoolAttributes();
    attributes.startDisabled = sd;
    return this;
  }

  @Override
  public void init(Pool cp) {
    setSocketConnectTimeout(cp.getSocketConnectTimeout());
    setFreeConnectionTimeout(cp.getFreeConnectionTimeout());
    setServerConnectionTimeout(cp.getServerConnectionTimeout());
    setLoadConditioningInterval(cp.getLoadConditioningInterval());
    setSocketBufferSize(cp.getSocketBufferSize());
    setReadTimeout(cp.getReadTimeout());
    setMinConnections(cp.getMinConnections());
    setMaxConnections(cp.getMaxConnections());
    setRetryAttempts(cp.getRetryAttempts());
    setIdleTimeout(cp.getIdleTimeout());
    setPingInterval(cp.getPingInterval());
    setStatisticInterval(cp.getStatisticInterval());
    setThreadLocalConnections(cp.getThreadLocalConnections());
    setSubscriptionEnabled(cp.getSubscriptionEnabled());
    setPRSingleHopEnabled(cp.getPRSingleHopEnabled());
    setSubscriptionRedundancy(cp.getSubscriptionRedundancy());
    setSubscriptionMessageTrackingTimeout(cp.getSubscriptionMessageTrackingTimeout());
    setSubscriptionAckInterval(cp.getSubscriptionAckInterval());
    setServerGroup(cp.getServerGroup());
    setMultiuserAuthentication(cp.getMultiuserAuthentication());
    setSocketFactory(cp.getSocketFactory());
    for (InetSocketAddress address : cp.getLocators()) {
      addLocator(address.getHostString(), address.getPort());
    }
    attributes.servers.addAll(cp.getServers().stream()
        .map(x -> new HostAndPort(x.getHostString(), x.getPort())).collect(Collectors.toList()));
  }

  public void init(GatewaySender sender) {
    attributes.setGateway(true);
    attributes.setGatewaySender(sender);
    setIdleTimeout(-1); // never time out
    setLoadConditioningInterval(-1); // never time out
    setMaxConnections(-1);
    setMinConnections(0);
  }

  /**
   * Create a new Pool for connecting a client to a set of GemFire Cache Servers. using this
   * factory's settings for attributes.
   *
   * @param name the name of the connection pool, used when connecting regions to it
   * @throws IllegalStateException if the connection pool name already exists
   * @throws IllegalStateException if this factory does not have any locators or servers
   * @return the newly created connection pool.
   * @since GemFire 5.7
   */
  @Override
  public Pool create(String name) throws CacheException {
    InternalDistributedSystem distributedSystem = InternalDistributedSystem.getAnyInstance();
    InternalCache cache = getInternalCache();
    ThreadsMonitoring threadMonitoring = null;
    if (cache != null) {
      threadMonitoring = cache.getDistributionManager().getThreadMonitoring();
      TypeRegistry registry = cache.getPdxRegistry();
      if (registry != null && !attributes.isGateway()) {
        registry.creatingPool();
      }
    }
    return PoolImpl.create(pm, name, attributes, locatorAddresses, distributedSystem,
        cache, threadMonitoring);
  }

  @SuppressWarnings("deprecation")
  private static GemFireCacheImpl getInternalCache() {
    return GemFireCacheImpl.getInstance();
  }

  @Override
  @VisibleForTesting
  public PoolAttributes getPoolAttributes() {
    return attributes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PoolFactoryImpl)) {
      return false;
    }
    PoolFactoryImpl that = (PoolFactoryImpl) o;
    return Objects.equals(attributes, that.attributes)
        && Objects.equals(new HashSet<>(locatorAddresses), new HashSet<>(that.locatorAddresses));
  }

  @Override
  public int hashCode() {
    return Objects.hash(attributes, locatorAddresses);
  }

  /**
   * Not a true pool just the attributes. Serialization is used by unit tests
   */
  public static class PoolAttributes implements Pool, Externalizable {

    private static final long serialVersionUID = 1L; // for findbugs

    int socketConnectTimeout = DEFAULT_SOCKET_CONNECT_TIMEOUT;
    SocketFactory socketFactory = DEFAULT_SOCKET_FACTORY;
    int connectionTimeout = DEFAULT_FREE_CONNECTION_TIMEOUT;
    int serverConnectionTimeout = DEFAULT_SERVER_CONNECTION_TIMEOUT;
    int connectionLifetime = DEFAULT_LOAD_CONDITIONING_INTERVAL;
    public int socketBufferSize = DEFAULT_SOCKET_BUFFER_SIZE;
    @Deprecated
    private boolean threadLocalConnections = DEFAULT_THREAD_LOCAL_CONNECTIONS;
    public int readTimeout = DEFAULT_READ_TIMEOUT;
    public int minConnections = DEFAULT_MIN_CONNECTIONS;
    public int maxConnections = DEFAULT_MAX_CONNECTIONS;
    public long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    public int retryAttempts = DEFAULT_RETRY_ATTEMPTS;
    public long pingInterval = DEFAULT_PING_INTERVAL;
    public int statisticInterval = DEFAULT_STATISTIC_INTERVAL;
    boolean queueEnabled = DEFAULT_SUBSCRIPTION_ENABLED;
    public boolean prSingleHopEnabled = DEFAULT_PR_SINGLE_HOP_ENABLED;
    int queueRedundancyLevel = DEFAULT_SUBSCRIPTION_REDUNDANCY;
    int queueMessageTrackingTimeout = DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT;
    int queueAckInterval = DEFAULT_SUBSCRIPTION_ACK_INTERVAL;
    int subscriptionTimeoutMultipler = DEFAULT_SUBSCRIPTION_TIMEOUT_MULTIPLIER;
    public String serverGroup = DEFAULT_SERVER_GROUP;
    boolean multiuserSecureModeEnabled = DEFAULT_MULTIUSER_AUTHENTICATION;
    public ArrayList<HostAndPort> locators = new ArrayList<>();
    public ArrayList<HostAndPort> servers = new ArrayList<>();
    public transient boolean startDisabled = false; // only used by junit tests
    public transient LocatorDiscoveryCallback locatorCallback = null; // only used by tests
    public GatewaySender gatewaySender = null;
    /**
     * True if the pool is used by a Gateway.
     */
    public boolean gateway = false;

    @Override
    public int getSocketConnectTimeout() {
      return socketConnectTimeout;
    }

    @Override
    public int getFreeConnectionTimeout() {
      return connectionTimeout;
    }

    @Override
    public int getServerConnectionTimeout() {
      return serverConnectionTimeout;
    }

    @Override
    public int getLoadConditioningInterval() {
      return connectionLifetime;
    }

    @Override
    public int getSocketBufferSize() {
      return socketBufferSize;
    }

    @Override
    public int getMinConnections() {
      return minConnections;
    }

    @Override
    public int getMaxConnections() {
      return maxConnections;
    }

    @Override
    public long getIdleTimeout() {
      return idleTimeout;
    }

    @Override
    public int getRetryAttempts() {
      return retryAttempts;
    }

    @Override
    public long getPingInterval() {
      return pingInterval;
    }

    @Override
    public int getStatisticInterval() {
      return statisticInterval;
    }

    @Override
    @Deprecated
    public boolean getThreadLocalConnections() {
      return threadLocalConnections;
    }

    @Override
    public int getReadTimeout() {
      return readTimeout;
    }

    @Override
    public boolean getSubscriptionEnabled() {
      return queueEnabled;
    }

    @Override
    public boolean getPRSingleHopEnabled() {
      return prSingleHopEnabled;
    }

    @Override
    public int getSubscriptionRedundancy() {
      return queueRedundancyLevel;
    }

    @Override
    public int getSubscriptionMessageTrackingTimeout() {
      return queueMessageTrackingTimeout;
    }

    @Override
    public int getSubscriptionAckInterval() {
      return queueAckInterval;
    }

    @Override
    public String getServerGroup() {
      return serverGroup;
    }

    public boolean isGateway() {
      return gateway;
    }

    public void setGateway(boolean v) {
      gateway = v;
    }

    public void setGatewaySender(GatewaySender sender) {
      gatewaySender = sender;
    }

    public GatewaySender getGatewaySender() {
      return gatewaySender;
    }

    @Override
    public boolean getMultiuserAuthentication() {
      return multiuserSecureModeEnabled;
    }

    public void setMultiuserSecureModeEnabled(boolean v) {
      multiuserSecureModeEnabled = v;
    }

    @Override
    public int getSubscriptionTimeoutMultiplier() {
      return subscriptionTimeoutMultipler;
    }

    @Override
    public SocketFactory getSocketFactory() {
      return socketFactory;
    }

    @Override
    public List<InetSocketAddress> getLocators() {
      if (locators.size() == 0 && servers.size() == 0) {
        throw new IllegalStateException(
            "At least one locator or server must be added before a connection pool can be created.");
      }
      return locators.stream().map(x -> x.getSocketInetAddress()).collect(Collectors.toList());
    }

    @Override
    public List<InetSocketAddress> getOnlineLocators() {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<InetSocketAddress> getServers() {
      if (locators.size() == 0 && servers.size() == 0) {
        throw new IllegalStateException(
            "At least one locator or server must be added before a connection pool can be created.");
      }
      // needs to return a copy.
      return servers.stream().map(x -> x.getSocketInetAddress()).collect(Collectors.toList());
    }

    @Override
    public String getName() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void destroy() throws CacheException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void destroy(boolean keepAlive) throws CacheException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDestroyed() {
      throw new UnsupportedOperationException();
    }

    @Override
    public QueryService getQueryService() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getPendingEventCount() {
      throw new UnsupportedOperationException();
    }


    public void toData(DataOutput out) throws IOException {
      DataSerializer.writePrimitiveInt(connectionTimeout, out);
      DataSerializer.writePrimitiveInt(serverConnectionTimeout, out);
      DataSerializer.writePrimitiveInt(connectionLifetime, out);
      DataSerializer.writePrimitiveInt(socketBufferSize, out);
      DataSerializer.writePrimitiveInt(readTimeout, out);
      DataSerializer.writePrimitiveInt(minConnections, out);
      DataSerializer.writePrimitiveInt(maxConnections, out);
      DataSerializer.writePrimitiveInt(retryAttempts, out);
      DataSerializer.writePrimitiveLong(idleTimeout, out);
      DataSerializer.writePrimitiveLong(pingInterval, out);
      DataSerializer.writePrimitiveInt(queueRedundancyLevel, out);
      DataSerializer.writePrimitiveInt(queueMessageTrackingTimeout, out);
      DataSerializer.writePrimitiveBoolean(threadLocalConnections, out);
      DataSerializer.writePrimitiveBoolean(queueEnabled, out);
      DataSerializer.writeString(serverGroup, out);
      DataSerializer.writeArrayList(locators, out);
      DataSerializer.writeArrayList(servers, out);
      DataSerializer.writePrimitiveInt(statisticInterval, out);
      DataSerializer.writePrimitiveBoolean(multiuserSecureModeEnabled, out);
      DataSerializer.writePrimitiveInt(socketConnectTimeout, out);
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      connectionTimeout = DataSerializer.readPrimitiveInt(in);
      serverConnectionTimeout = DataSerializer.readPrimitiveInt(in);
      connectionLifetime = DataSerializer.readPrimitiveInt(in);
      socketBufferSize = DataSerializer.readPrimitiveInt(in);
      readTimeout = DataSerializer.readPrimitiveInt(in);
      minConnections = DataSerializer.readPrimitiveInt(in);
      maxConnections = DataSerializer.readPrimitiveInt(in);
      retryAttempts = DataSerializer.readPrimitiveInt(in);
      idleTimeout = DataSerializer.readPrimitiveLong(in);
      pingInterval = DataSerializer.readPrimitiveLong(in);
      queueRedundancyLevel = DataSerializer.readPrimitiveInt(in);
      queueMessageTrackingTimeout = DataSerializer.readPrimitiveInt(in);
      threadLocalConnections = DataSerializer.readPrimitiveBoolean(in);
      queueEnabled = DataSerializer.readPrimitiveBoolean(in);
      serverGroup = DataSerializer.readString(in);
      locators = DataSerializer.readArrayList(in);
      servers = DataSerializer.readArrayList(in);
      statisticInterval = DataSerializer.readPrimitiveInt(in);
      multiuserSecureModeEnabled = DataSerializer.readPrimitiveBoolean(in);
      socketConnectTimeout = DataSerializer.readPrimitiveInt(in);
    }

    @Override
    public int hashCode() {
      return Objects
          .hash(socketConnectTimeout, connectionTimeout, serverConnectionTimeout,
              connectionLifetime, socketBufferSize,
              threadLocalConnections, readTimeout, minConnections, maxConnections, idleTimeout,
              retryAttempts, pingInterval, statisticInterval, queueEnabled, prSingleHopEnabled,
              queueRedundancyLevel, queueMessageTrackingTimeout, queueAckInterval,
              subscriptionTimeoutMultipler, serverGroup, multiuserSecureModeEnabled, locators,
              servers, startDisabled, locatorCallback, gatewaySender, gateway, socketFactory);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PoolAttributes)) {
        return false;
      }
      PoolAttributes that = (PoolAttributes) o;
      return socketConnectTimeout == that.socketConnectTimeout
          && connectionTimeout == that.connectionTimeout
          && serverConnectionTimeout == that.serverConnectionTimeout
          && connectionLifetime == that.connectionLifetime
          && socketBufferSize == that.socketBufferSize
          && threadLocalConnections == that.threadLocalConnections
          && readTimeout == that.readTimeout && minConnections == that.minConnections
          && maxConnections == that.maxConnections && idleTimeout == that.idleTimeout
          && retryAttempts == that.retryAttempts && pingInterval == that.pingInterval
          && statisticInterval == that.statisticInterval && queueEnabled == that.queueEnabled
          && prSingleHopEnabled == that.prSingleHopEnabled
          && queueRedundancyLevel == that.queueRedundancyLevel
          && queueMessageTrackingTimeout == that.queueMessageTrackingTimeout
          && queueAckInterval == that.queueAckInterval
          && multiuserSecureModeEnabled == that.multiuserSecureModeEnabled
          && startDisabled == that.startDisabled && gateway == that.gateway
          && Objects.equals(serverGroup, that.serverGroup)
          && Objects.equals(new HashSet<>(locators), new HashSet<>(that.locators))
          && Objects.equals(new HashSet<>(servers), new HashSet<>(that.servers))
          && Objects.equals(locatorCallback, that.locatorCallback)
          && Objects.equals(gatewaySender, that.gatewaySender)
          && Objects.equals(socketFactory, that.socketFactory);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      toData(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      fromData(in);
    }
  }
}
