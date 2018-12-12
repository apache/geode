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
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.internal.LocatorDiscoveryCallback;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.gms.membership.HostAddress;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * Implementation of PoolFactory.
 *
 * @since GemFire 5.7
 */
public class PoolFactoryImpl implements PoolFactory {
  private static final Logger logger = LogService.getLogger();

  /**
   * Used internally to pass the attributes from this factory to the real pool it is creating.
   */
  private PoolAttributes attributes = new PoolAttributes();

  private List<HostAddress> locatorAddresses = new ArrayList<>();

  /**
   * The cache that created this factory
   */
  private final PoolManagerImpl pm;

  public PoolFactoryImpl(PoolManagerImpl pm) {
    this.pm = pm;
  }

  public PoolFactory setSocketConnectTimeout(int socketConnectTimeout) {
    if (socketConnectTimeout <= -1) {
      throw new IllegalArgumentException("socketConnectTimeout must be greater than -1");
    }
    this.attributes.socketConnectTimeout = socketConnectTimeout;
    return this;
  }

  public PoolFactory setFreeConnectionTimeout(int connectionTimeout) {
    if (connectionTimeout <= 0) {
      throw new IllegalArgumentException("connectionTimeout must be greater than zero");
    }
    this.attributes.connectionTimeout = connectionTimeout;
    return this;
  }

  public PoolFactory setLoadConditioningInterval(int connectionLifetime) {
    if (connectionLifetime < -1) {
      throw new IllegalArgumentException("connectionLifetime must be greater than or equal to -1");
    }
    this.attributes.connectionLifetime = connectionLifetime;
    return this;
  }

  public PoolFactory setSocketBufferSize(int bufferSize) {
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("socketBufferSize must be greater than zero");
    }
    this.attributes.socketBufferSize = bufferSize;
    return this;
  }

  public PoolFactory setThreadLocalConnections(boolean threadLocalConnections) {
    this.attributes.threadLocalConnections = threadLocalConnections;
    return this;
  }

  public PoolFactory setIdleTimeout(long idleTimout) {
    if (idleTimout < -1) {
      throw new IllegalArgumentException("idleTimeout must be greater than or equal to -1");
    }
    this.attributes.idleTimeout = idleTimout;
    return this;
  }

  public PoolFactory setMaxConnections(int maxConnections) {
    if (maxConnections < this.attributes.minConnections && maxConnections != -1) {
      throw new IllegalArgumentException(
          "maxConnections must be greater than or equal to minConnections ("
              + attributes.minConnections + ")");
    }
    if (maxConnections <= 0 && maxConnections != -1) {
      throw new IllegalArgumentException(
          "maxConnections must be greater than 0, or set to -1 (no max)");
    }
    this.attributes.maxConnections = maxConnections;
    return this;
  }

  public PoolFactory setMinConnections(int minConnections) {
    if (minConnections > attributes.maxConnections && attributes.maxConnections != -1) {
      throw new IllegalArgumentException(
          "must be less than or equal to maxConnections (" + attributes.maxConnections + ")");
    }
    if (minConnections < 0) {
      throw new IllegalArgumentException("must be greater than or equal to 0");
    }
    this.attributes.minConnections = minConnections;
    return this;
  }

  public PoolFactory setPingInterval(long pingInterval) {
    if (pingInterval <= 0) {
      throw new IllegalArgumentException("pingInterval must be greater than zero");
    }
    this.attributes.pingInterval = pingInterval;
    return this;
  }

  public PoolFactory setStatisticInterval(int statisticInterval) {
    if (statisticInterval < -1) {
      throw new IllegalArgumentException("statisticInterval must be greater than or equal to -1");
    }
    this.attributes.statisticInterval = statisticInterval;
    return this;
  }

  public PoolFactory setRetryAttempts(int retryAttempts) {
    if (retryAttempts < -1) {
      throw new IllegalArgumentException("retryAttempts must be greater than or equal to -1");
    }
    this.attributes.retryAttempts = retryAttempts;
    return this;
  }

  public PoolFactory setReadTimeout(int timeout) {
    if (timeout < 0) {
      throw new IllegalArgumentException("readTimeout must be greater than or equal to zero");
    }
    this.attributes.readTimeout = timeout;
    return this;
  }

  public PoolFactory setServerGroup(String group) {
    if (group == null) {
      group = DEFAULT_SERVER_GROUP;
    }
    this.attributes.serverGroup = group;
    return this;
  }

  public PoolFactory setSubscriptionEnabled(boolean enabled) {
    this.attributes.queueEnabled = enabled;
    return this;
  }

  public PoolFactory setPRSingleHopEnabled(boolean enabled) {
    this.attributes.prSingleHopEnabled = enabled;
    return this;
  }

  public PoolFactory setMultiuserAuthentication(boolean enabled) {
    this.attributes.multiuserSecureModeEnabled = enabled;
    return this;
  }

  public PoolFactory setStartDisabled(boolean disable) {
    this.attributes.startDisabled = disable;
    return this;
  }

  public PoolFactory setLocatorDiscoveryCallback(LocatorDiscoveryCallback callback) {
    this.attributes.locatorCallback = callback;
    return this;
  }

  public PoolFactory setSubscriptionRedundancy(int redundancyLevel) {
    if (redundancyLevel < -1) {
      throw new IllegalArgumentException(
          "queueRedundancyLevel must be greater than or equal to -1");
    }
    this.attributes.queueRedundancyLevel = redundancyLevel;
    return this;
  }

  public PoolFactory setSubscriptionMessageTrackingTimeout(int messageTrackingTimeout) {
    if (messageTrackingTimeout <= 0) {
      throw new IllegalArgumentException("queueMessageTrackingTimeout must be greater than zero");
    }
    this.attributes.queueMessageTrackingTimeout = messageTrackingTimeout;
    return this;
  }

  @Override
  public PoolFactory setSubscriptionTimeoutMultiplier(int multiplier) {
    this.attributes.subscriptionTimeoutMultipler = multiplier;
    return this;
  }

  private InetSocketAddress getInetSocketAddress(String host, int port) {
    if (port == 0) {
      throw new IllegalArgumentException("port must be greater than 0 but was " + port);
      // the rest of the port validation is done by InetSocketAddress
    }
    InetSocketAddress sockAddr = null;
    try {
      InetAddress hostAddr = InetAddress.getByName(host);
      sockAddr = new InetSocketAddress(hostAddr, port);
    } catch (UnknownHostException ignore) {
      // IllegalArgumentException ex = new IllegalArgumentException("Unknown host " + host);
      // ex.initCause(cause);
      // throw ex;
      // Fix for #45348
      logger.warn(
          "Hostname is unknown: {}. Creating pool with unknown host in case the host becomes known later.",
          host);
      sockAddr = new InetSocketAddress(host, port);
    }
    return sockAddr;
  }

  public PoolFactory setSubscriptionAckInterval(int ackInterval) {
    if (ackInterval <= 0) {
      throw new IllegalArgumentException("ackInterval must be greater than 0");
    }
    this.attributes.queueAckInterval = ackInterval;

    return this;
  }

  public PoolFactory addLocator(String host, int port) {
    if (this.attributes.servers.size() > 0) {
      throw new IllegalStateException(
          "A server has already been added. You can only add locators or servers; not both.");
    }
    InetSocketAddress isa = getInetSocketAddress(host, port);
    this.attributes.locators.add(isa);
    locatorAddresses.add(new HostAddress(isa, host));
    return this;
  }

  public PoolFactory addServer(String host, int port) {
    if (this.attributes.locators.size() > 0) {
      throw new IllegalStateException(
          "A locator has already been added. You can only add locators or servers; not both.");
    }
    this.attributes.servers.add(getInetSocketAddress(host, port));
    return this;
  }

  public PoolFactory reset() {
    // preserve the startDisabled across resets
    boolean sd = this.attributes.startDisabled;
    this.attributes = new PoolAttributes();
    this.attributes.startDisabled = sd;
    return this;
  }


  /**
   * Initializes the state of this factory for the given pool's state.
   */
  public void init(Pool cp) {
    setSocketConnectTimeout(cp.getSocketConnectTimeout());
    setFreeConnectionTimeout(cp.getFreeConnectionTimeout());
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
    for (InetSocketAddress inetSocketAddress : cp.getLocators()) {
      addLocator(inetSocketAddress.getHostName(), inetSocketAddress.getPort());
    }
    this.attributes.servers.addAll(cp.getServers());
  }

  public void init(GatewaySender sender) {
    this.attributes.setGateway(true);
    this.attributes.setGatewaySender(sender);
    setIdleTimeout(-1); // never time out
    setLoadConditioningInterval(-1); // never time out
    setMaxConnections(-1);
    setMinConnections(0);
    setThreadLocalConnections(true);
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
  public Pool create(String name) throws CacheException {
    InternalDistributedSystem distributedSystem = InternalDistributedSystem.getAnyInstance();
    InternalCache cache = GemFireCacheImpl.getInstance();
    ThreadsMonitoring threadMonitoring = null;
    if (cache != null) {
      threadMonitoring = cache.getDistributionManager().getThreadMonitoring();
      TypeRegistry registry = cache.getPdxRegistry();
      if (registry != null && !attributes.isGateway()) {
        registry.creatingPool();
      }
    }
    return PoolImpl.create(this.pm, name, this.attributes, this.locatorAddresses, distributedSystem,
        cache, threadMonitoring);
  }

  /**
   * Needed by test framework.
   */
  public PoolAttributes getPoolAttributes() {
    return this.attributes;
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
        && Objects.equals(new HashSet(locatorAddresses), new HashSet(that.locatorAddresses));
  }

  /**
   * Not a true pool just the attributes. Serialization is used by unit tests
   */
  public static class PoolAttributes implements Pool, DataSerializable {

    private static final long serialVersionUID = 1L; // for findbugs

    public int socketConnectTimeout = DEFAULT_SOCKET_CONNECT_TIMEOUT;
    public int connectionTimeout = DEFAULT_FREE_CONNECTION_TIMEOUT;
    public int connectionLifetime = DEFAULT_LOAD_CONDITIONING_INTERVAL;
    public int socketBufferSize = DEFAULT_SOCKET_BUFFER_SIZE;
    public boolean threadLocalConnections = DEFAULT_THREAD_LOCAL_CONNECTIONS;
    public int readTimeout = DEFAULT_READ_TIMEOUT;
    public int minConnections = DEFAULT_MIN_CONNECTIONS;
    public int maxConnections = DEFAULT_MAX_CONNECTIONS;
    public long idleTimeout = DEFAULT_IDLE_TIMEOUT;
    public int retryAttempts = DEFAULT_RETRY_ATTEMPTS;
    public long pingInterval = DEFAULT_PING_INTERVAL;
    public int statisticInterval = DEFAULT_STATISTIC_INTERVAL;
    public boolean queueEnabled = DEFAULT_SUBSCRIPTION_ENABLED;
    public boolean prSingleHopEnabled = DEFAULT_PR_SINGLE_HOP_ENABLED;
    public int queueRedundancyLevel = DEFAULT_SUBSCRIPTION_REDUNDANCY;
    public int queueMessageTrackingTimeout = DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT;
    public int queueAckInterval = DEFAULT_SUBSCRIPTION_ACK_INTERVAL;
    public int subscriptionTimeoutMultipler = DEFAULT_SUBSCRIPTION_TIMEOUT_MULTIPLIER;
    public String serverGroup = DEFAULT_SERVER_GROUP;
    public boolean multiuserSecureModeEnabled = DEFAULT_MULTIUSER_AUTHENTICATION;
    public ArrayList/* <InetSocketAddress> */ locators = new ArrayList();
    public ArrayList/* <InetSocketAddress> */ servers = new ArrayList();
    public transient boolean startDisabled = false; // only used by junit tests
    public transient LocatorDiscoveryCallback locatorCallback = null; // only used by tests
    public GatewaySender gatewaySender = null;
    /**
     * True if the pool is used by a Gateway.
     */
    public boolean gateway = false;

    @Override
    public int getSocketConnectTimeout() {
      return this.socketConnectTimeout;
    }

    @Override
    public int getFreeConnectionTimeout() {
      return this.connectionTimeout;
    }

    @Override
    public int getLoadConditioningInterval() {
      return this.connectionLifetime;
    }

    @Override
    public int getSocketBufferSize() {
      return this.socketBufferSize;
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
    public boolean getThreadLocalConnections() {
      return this.threadLocalConnections;
    }

    @Override
    public int getReadTimeout() {
      return this.readTimeout;
    }

    @Override
    public boolean getSubscriptionEnabled() {
      return this.queueEnabled;
    }

    @Override
    public boolean getPRSingleHopEnabled() {
      return this.prSingleHopEnabled;
    }

    @Override
    public int getSubscriptionRedundancy() {
      return this.queueRedundancyLevel;
    }

    @Override
    public int getSubscriptionMessageTrackingTimeout() {
      return this.queueMessageTrackingTimeout;
    }

    @Override
    public int getSubscriptionAckInterval() {
      return queueAckInterval;
    }

    @Override
    public String getServerGroup() {
      return this.serverGroup;
    }

    public boolean isGateway() {
      return this.gateway;
    }

    public void setGateway(boolean v) {
      this.gateway = v;
    }

    public void setGatewaySender(GatewaySender sender) {
      this.gatewaySender = sender;
    }

    public GatewaySender getGatewaySender() {
      return this.gatewaySender;
    }

    @Override
    public boolean getMultiuserAuthentication() {
      return this.multiuserSecureModeEnabled;
    }

    public void setMultiuserSecureModeEnabled(boolean v) {
      this.multiuserSecureModeEnabled = v;
    }

    @Override
    public int getSubscriptionTimeoutMultiplier() {
      return this.subscriptionTimeoutMultipler;
    }

    public List/* <InetSocketAddress> */ getLocators() {
      if (this.locators.size() == 0 && this.servers.size() == 0) {
        throw new IllegalStateException(
            "At least one locator or server must be added before a connection pool can be created.");
      }
      // needs to return a copy.
      return Collections.unmodifiableList(new ArrayList(this.locators));
    }

    @Override
    public List<InetSocketAddress> getOnlineLocators() {
      throw new UnsupportedOperationException();
    }

    public List/* <InetSocketAddress> */ getServers() {
      if (this.locators.size() == 0 && this.servers.size() == 0) {
        throw new IllegalStateException(
            "At least one locator or server must be added before a connection pool can be created.");
      }
      // needs to return a copy.
      return Collections.unmodifiableList(new ArrayList(this.servers));
    }

    public String getName() {
      throw new UnsupportedOperationException();
    }

    public void destroy() throws CacheException {
      throw new UnsupportedOperationException();
    }

    public void destroy(boolean keepAlive) throws CacheException {
      throw new UnsupportedOperationException();
    }

    public boolean isDestroyed() {
      throw new UnsupportedOperationException();
    }

    public void releaseThreadLocalConnection() {
      throw new UnsupportedOperationException();
    }

    public QueryService getQueryService() {
      throw new UnsupportedOperationException();
    }

    public int getPendingEventCount() {
      throw new UnsupportedOperationException();
    }


    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writePrimitiveInt(this.connectionTimeout, out);
      DataSerializer.writePrimitiveInt(this.connectionLifetime, out);
      DataSerializer.writePrimitiveInt(this.socketBufferSize, out);
      DataSerializer.writePrimitiveInt(this.readTimeout, out);
      DataSerializer.writePrimitiveInt(this.minConnections, out);
      DataSerializer.writePrimitiveInt(this.maxConnections, out);
      DataSerializer.writePrimitiveInt(this.retryAttempts, out);
      DataSerializer.writePrimitiveLong(this.idleTimeout, out);
      DataSerializer.writePrimitiveLong(this.pingInterval, out);
      DataSerializer.writePrimitiveInt(this.queueRedundancyLevel, out);
      DataSerializer.writePrimitiveInt(this.queueMessageTrackingTimeout, out);
      DataSerializer.writePrimitiveBoolean(this.threadLocalConnections, out);
      DataSerializer.writePrimitiveBoolean(this.queueEnabled, out);
      DataSerializer.writeString(this.serverGroup, out);
      DataSerializer.writeArrayList(this.locators, out);
      DataSerializer.writeArrayList(this.servers, out);
      DataSerializer.writePrimitiveInt(this.statisticInterval, out);
      DataSerializer.writePrimitiveBoolean(this.multiuserSecureModeEnabled, out);
      DataSerializer.writePrimitiveInt(this.socketConnectTimeout, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.connectionTimeout = DataSerializer.readPrimitiveInt(in);
      this.connectionLifetime = DataSerializer.readPrimitiveInt(in);
      this.socketBufferSize = DataSerializer.readPrimitiveInt(in);
      this.readTimeout = DataSerializer.readPrimitiveInt(in);
      this.minConnections = DataSerializer.readPrimitiveInt(in);
      this.maxConnections = DataSerializer.readPrimitiveInt(in);
      this.retryAttempts = DataSerializer.readPrimitiveInt(in);
      this.idleTimeout = DataSerializer.readPrimitiveLong(in);
      this.pingInterval = DataSerializer.readPrimitiveLong(in);
      this.queueRedundancyLevel = DataSerializer.readPrimitiveInt(in);
      this.queueMessageTrackingTimeout = DataSerializer.readPrimitiveInt(in);
      this.threadLocalConnections = DataSerializer.readPrimitiveBoolean(in);
      this.queueEnabled = DataSerializer.readPrimitiveBoolean(in);
      this.serverGroup = DataSerializer.readString(in);
      this.locators = DataSerializer.readArrayList(in);
      this.servers = DataSerializer.readArrayList(in);
      this.statisticInterval = DataSerializer.readPrimitiveInt(in);
      this.multiuserSecureModeEnabled = DataSerializer.readPrimitiveBoolean(in);
      this.socketConnectTimeout = DataSerializer.readPrimitiveInt(in);
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
          && Objects.equals(new HashSet(locators), new HashSet(that.locators))
          && Objects.equals(new HashSet(servers), new HashSet(that.servers))
          && Objects.equals(locatorCallback, that.locatorCallback)
          && Objects.equals(gatewaySender, that.gatewaySender);
    }
  }
}
