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

import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.NoSubscriptionServersAvailableException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.SubscriptionNotEnabledException;
import org.apache.geode.cache.client.internal.pooling.ConnectionManager;
import org.apache.geode.cache.client.internal.pooling.ConnectionManagerImpl;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.internal.DefaultQueryService;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.PoolCancelledException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.tcpserver.LocatorAddress;
import org.apache.geode.internal.admin.ClientStatsManager;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PoolFactoryImpl;
import org.apache.geode.internal.cache.PoolManagerImpl;
import org.apache.geode.internal.cache.PoolStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.logging.CoreLoggingExecutors;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.internal.monitoring.ThreadsMonitoring;
import org.apache.geode.internal.statistics.DummyStatisticsFactory;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Manages the client side of client to server connections and client queues.
 *
 * @since GemFire 5.7
 */
public class PoolImpl implements InternalPool {

  public static final String ON_DISCONNECT_CLEAR_PDXTYPEIDS =
      GeodeGlossary.GEMFIRE_PREFIX + "ON_DISCONNECT_CLEAR_PDXTYPEIDS";

  private static final Logger logger = LogService.getLogger();

  public static final long SHUTDOWN_TIMEOUT =
      Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.SHUTDOWN_TIMEOUT", 30000);

  private static final int BACKGROUND_TASK_POOL_SIZE = Integer
      .getInteger(GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.BACKGROUND_TASK_POOL_SIZE", 20);

  private static final int BACKGROUND_TASK_POOL_KEEP_ALIVE = Integer.getInteger(
      GeodeGlossary.GEMFIRE_PREFIX + "PoolImpl.BACKGROUND_TASK_POOL_KEEP_ALIVE", 1000);

  /**
   * For durable client tests only. Connection Sources read this flag and return an empty list of
   * servers.
   */
  @MutableForTesting
  static volatile boolean TEST_DURABLE_IS_NET_DOWN = false;

  private final String name;
  private final int socketConnectTimeout;
  private final int freeConnectionTimeout;
  private final int loadConditioningInterval;
  private final int socketBufferSize;
  @Deprecated
  private final boolean threadLocalConnections;
  private final int readTimeout;
  private final boolean subscriptionEnabled;
  private final boolean prSingleHopEnabled;
  private final int subscriptionRedundancyLevel;
  private final int subscriptionMessageTrackingTimeout;
  private final int subscriptionAckInterval;
  private final int subscriptionTimeoutMultiplier;
  private final String serverGroup;
  private final List<LocatorAddress> locatorAddresses;
  private final List<InetSocketAddress> locators;
  private final List<InetSocketAddress> servers;
  private final boolean startDisabled;
  private final boolean usedByGateway;
  private final int maxConnections;
  private final int minConnections;
  private final int retryAttempts;
  private final long idleTimeout;
  private final long pingInterval;
  private final int statisticInterval;
  private final boolean multiuserSecureModeEnabled;

  private final ConnectionSource source;
  private final ConnectionManager manager;
  private QueueManager queueManager;
  protected final EndpointManager endpointManager;
  private final PoolManagerImpl pm;
  protected final InternalLogWriter securityLogWriter;
  protected volatile boolean destroyed;
  private final PoolStats stats;
  private ScheduledExecutorService backgroundProcessor;
  private final OpExecutorImpl executor;
  private final RegisterInterestTracker riTracker = new RegisterInterestTracker();
  private final InternalDistributedSystem distributedSystem;
  private InternalCache cache;

  private final ClientProxyMembershipID proxyId;
  protected final CancelCriterion cancelCriterion;
  private final ConnectionFactoryImpl connectionFactory;

  private final ArrayList<ProxyCache> proxyCacheList;

  private final GatewaySender gatewaySender;

  private boolean keepAlive = false;
  private static final Object simpleLock = new Object();

  public static final int PRIMARY_QUEUE_NOT_AVAILABLE = -2;
  public static final int PRIMARY_QUEUE_TIMED_OUT = -1;
  private final AtomicInteger primaryQueueSize = new AtomicInteger(PRIMARY_QUEUE_NOT_AVAILABLE);

  private final ThreadsMonitoring threadMonitoring;

  public static PoolImpl create(PoolManagerImpl pm, String name, Pool attributes,
      List<LocatorAddress> locatorAddresses, InternalDistributedSystem distributedSystem,
      InternalCache cache, ThreadsMonitoring tMonitoring) {
    PoolImpl pool =
        new PoolImpl(pm, name, attributes, locatorAddresses, distributedSystem, cache, tMonitoring);
    pool.finishCreate(pm);
    return pool;
  }

  public boolean isUsedByGateway() {
    return usedByGateway;
  }

  /**
   * @since GemFire 5.7
   */
  private void finishCreate(PoolManagerImpl pm) {
    pm.register(this);
    try {
      start();
    } catch (RuntimeException e) {
      try {
        destroy(false);
      } catch (RuntimeException ignore) {
        // do nothing
      }
      throw e;
    }
  }

  protected PoolImpl(PoolManagerImpl pm, String name, Pool attributes,
      List<LocatorAddress> locatorAddresses, InternalDistributedSystem distributedSystem,
      InternalCache cache, ThreadsMonitoring threadMonitoring) {
    this.pm = pm;
    this.name = name;
    this.locatorAddresses = locatorAddresses;
    if (distributedSystem == null) {
      throw new IllegalStateException(
          "Distributed System must be created before creating pool");
    }
    this.distributedSystem = distributedSystem;
    this.cache = cache;
    this.threadMonitoring = threadMonitoring;

    socketConnectTimeout = attributes.getSocketConnectTimeout();
    freeConnectionTimeout = attributes.getFreeConnectionTimeout();
    loadConditioningInterval = attributes.getLoadConditioningInterval();
    socketBufferSize = attributes.getSocketBufferSize();
    threadLocalConnections = attributes.getThreadLocalConnections();
    readTimeout = attributes.getReadTimeout();
    minConnections = attributes.getMinConnections();
    maxConnections = attributes.getMaxConnections();
    retryAttempts = attributes.getRetryAttempts();
    idleTimeout = attributes.getIdleTimeout();
    pingInterval = attributes.getPingInterval();
    statisticInterval = attributes.getStatisticInterval();
    subscriptionEnabled = attributes.getSubscriptionEnabled();
    prSingleHopEnabled = attributes.getPRSingleHopEnabled();
    subscriptionRedundancyLevel = attributes.getSubscriptionRedundancy();
    subscriptionMessageTrackingTimeout = attributes.getSubscriptionMessageTrackingTimeout();
    subscriptionAckInterval = attributes.getSubscriptionAckInterval();
    subscriptionTimeoutMultiplier = attributes.getSubscriptionTimeoutMultiplier();
    if (subscriptionTimeoutMultiplier < 0) {
      throw new IllegalArgumentException(
          "The subscription timeout multiplier must not be negative");
    }
    serverGroup = attributes.getServerGroup();
    multiuserSecureModeEnabled = attributes.getMultiuserAuthentication();
    locators = attributes.getLocators();
    servers = attributes.getServers();
    startDisabled =
        ((PoolFactoryImpl.PoolAttributes) attributes).startDisabled || !pm.isNormal();
    usedByGateway = ((PoolFactoryImpl.PoolAttributes) attributes).isGateway();
    gatewaySender = ((PoolFactoryImpl.PoolAttributes) attributes).getGatewaySender();
    securityLogWriter = distributedSystem.getSecurityInternalLogWriter();
    if (!distributedSystem.getConfig().getStatisticSamplingEnabled() && statisticInterval > 0) {
      logger.info("statistic-sampling must be enabled for sampling rate of {} to take affect",
          statisticInterval);
    }
    cancelCriterion = new Stopper();
    if (Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "SPECIAL_DURABLE")) {
      ClientProxyMembershipID.setPoolName(name);
      proxyId = ClientProxyMembershipID.getNewProxyMembership(distributedSystem);
      ClientProxyMembershipID.setPoolName(null);
    } else {
      proxyId = ClientProxyMembershipID.getNewProxyMembership(distributedSystem);
    }
    StatisticsFactory statFactory;
    if (gatewaySender != null) {
      statFactory = new DummyStatisticsFactory();
    } else {
      statFactory = distributedSystem;
    }
    stats = startDisabled ? null
        : new PoolStats(statFactory, getName() + "->"
            + (isEmpty(serverGroup) ? "[any servers]" : "[" + getServerGroup() + "]"));

    source = getSourceImpl(((PoolFactoryImpl.PoolAttributes) attributes).locatorCallback);
    endpointManager = new EndpointManagerImpl(name, distributedSystem, cancelCriterion,
        stats);
    connectionFactory = new ConnectionFactoryImpl(source, endpointManager, distributedSystem,
        socketBufferSize, socketConnectTimeout, readTimeout, proxyId, cancelCriterion,
        usedByGateway, gatewaySender, pingInterval, multiuserSecureModeEnabled, this,
        distributedSystem.getConfig());
    if (subscriptionEnabled) {
      queueManager = new QueueManagerImpl(this, endpointManager, source, connectionFactory,
          subscriptionRedundancyLevel, pingInterval, securityLogWriter, proxyId);
    }

    manager = new ConnectionManagerImpl(name, connectionFactory, endpointManager, maxConnections,
        minConnections, idleTimeout, loadConditioningInterval, securityLogWriter, pingInterval,
        cancelCriterion, getStats());
    // Fix for 43468 - make sure we check the cache cancel criterion if we get
    // an exception, by passing in the poolOrCache stopper
    executor = new OpExecutorImpl(manager, queueManager, endpointManager, riTracker, retryAttempts,
        freeConnectionTimeout, new PoolOrCacheStopper(), this);
    if (multiuserSecureModeEnabled) {
      proxyCacheList = new ArrayList<>();
    } else {
      proxyCacheList = null;
    }
  }

  /**
   * Return true if the given Pool is compatible with these attributes. Currently this does what
   * equals would but in the future we might decide to weaken the compatibility contract.
   *
   * @since GemFire 6.5
   */
  public boolean isCompatible(Pool p) {
    if (p == null)
      return false;
    return getFreeConnectionTimeout() == p.getFreeConnectionTimeout()
        && getSocketConnectTimeout() == p.getSocketConnectTimeout()
        && getLoadConditioningInterval() == p.getLoadConditioningInterval()
        && getSocketBufferSize() == p.getSocketBufferSize()
        && getMinConnections() == p.getMinConnections()
        && getMaxConnections() == p.getMaxConnections() && getIdleTimeout() == p.getIdleTimeout()
        && getPingInterval() == p.getPingInterval()
        && getStatisticInterval() == p.getStatisticInterval()
        && getRetryAttempts() == p.getRetryAttempts()
        && getThreadLocalConnections() == p.getThreadLocalConnections()
        && getReadTimeout() == p.getReadTimeout()
        && getSubscriptionEnabled() == p.getSubscriptionEnabled()
        && getPRSingleHopEnabled() == p.getPRSingleHopEnabled()
        && getSubscriptionRedundancy() == p.getSubscriptionRedundancy()
        && getSubscriptionMessageTrackingTimeout() == p.getSubscriptionMessageTrackingTimeout()
        && getSubscriptionAckInterval() == p.getSubscriptionAckInterval()
        && getServerGroup().equals(p.getServerGroup())
        && getMultiuserAuthentication() == p.getMultiuserAuthentication()
        && getLocators().equals(p.getLocators()) && getServers().equals(p.getServers());
  }

  private void start() {
    if (startDisabled)
      return;

    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      List locators = getLocators();
      if (!locators.isEmpty()) {
        logger.debug("PoolImpl - starting pool with locators: {}", locators);
      } else {
        logger.debug("PoolImpl -starting pool with servers: {}", getServers());
      }
    }

    final String timerName = "poolTimer-" + getName() + "-";
    backgroundProcessor = CoreLoggingExecutors.newScheduledThreadPool(timerName,
        BACKGROUND_TASK_POOL_SIZE, BACKGROUND_TASK_POOL_KEEP_ALIVE, threadMonitoring);
    source.start(this);
    connectionFactory.start(backgroundProcessor);
    endpointManager.addListener(new InstantiatorRecoveryListener(backgroundProcessor, this));
    endpointManager.addListener(new DataSerializerRecoveryListener(backgroundProcessor, this));
    if (Boolean.getBoolean(ON_DISCONNECT_CLEAR_PDXTYPEIDS)) {
      endpointManager.addListener(new PdxRegistryRecoveryListener(this));
    }
    endpointManager.addListener(new LiveServerPinger(this));

    manager.start(backgroundProcessor);
    if (queueManager != null) {
      if (isDebugEnabled) {
        logger.debug("starting queueManager");
      }
      queueManager.start(backgroundProcessor);
    }
    if (isDebugEnabled) {
      logger.debug("scheduling pings every {} milliseconds", pingInterval);
    }


    if (statisticInterval > 0 && distributedSystem.getConfig().getStatisticSamplingEnabled()) {
      backgroundProcessor.scheduleWithFixedDelay(new PublishClientStatsTask(), statisticInterval,
          statisticInterval, TimeUnit.MILLISECONDS);
    }
    // LOG: changed from config to info
    logger.info("Pool {} started with multiuser-authentication={}",
        new Object[] {name, multiuserSecureModeEnabled});
  }

  /**
   * Returns the cancellation criterion for this proxy
   *
   * @return the cancellation criterion
   */
  @Override
  public CancelCriterion getCancelCriterion() {
    return cancelCriterion;
  }

  @Override
  public void setupServerAffinity(boolean allowFailover) {
    executor.setupServerAffinity(allowFailover);
  }

  @Override
  public void releaseServerAffinity() {
    executor.releaseServerAffinity();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getSocketConnectTimeout() {
    return socketConnectTimeout;
  }

  @Override
  public int getFreeConnectionTimeout() {
    return freeConnectionTimeout;
  }

  @Override
  public int getLoadConditioningInterval() {
    return loadConditioningInterval;
  }

  @Override
  public int getMaxConnections() {
    return maxConnections;
  }

  @Override
  public int getMinConnections() {
    return minConnections;
  }

  @Override
  public int getRetryAttempts() {
    return retryAttempts;
  }

  @Override
  public long getIdleTimeout() {
    return idleTimeout;
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
  public int getSocketBufferSize() {
    return socketBufferSize;
  }

  @Override
  public boolean getThreadLocalConnections() {
    return threadLocalConnections;
  }

  @Override
  public int getReadTimeout() {
    return readTimeout;
  }

  @Override
  public boolean getSubscriptionEnabled() {
    return subscriptionEnabled;
  }

  @Override
  public boolean getPRSingleHopEnabled() {
    return prSingleHopEnabled;
  }

  @Override
  public int getSubscriptionRedundancy() {
    return subscriptionRedundancyLevel;
  }

  @Override
  public int getSubscriptionMessageTrackingTimeout() {
    return subscriptionMessageTrackingTimeout;
  }

  @Override
  public int getSubscriptionAckInterval() {
    return subscriptionAckInterval;
  }

  @Override
  public String getServerGroup() {
    return serverGroup;
  }

  @Override
  public boolean getMultiuserAuthentication() {
    return multiuserSecureModeEnabled;
  }

  @Override
  public List<InetSocketAddress> getLocators() {
    return locators;
  }

  @Override
  public List<InetSocketAddress> getOnlineLocators() {
    return source.getOnlineLocators();
  }

  @Override
  public List<InetSocketAddress> getServers() {
    return servers;
  }

  public GatewaySender getGatewaySender() {
    return gatewaySender;
  }

  @Override
  public void destroy() {
    destroy(false);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + '@' + System.identityHashCode(this) + " name=" + getName();
  }

  @Override
  public void destroy(boolean keepAlive) {
    int cnt = getAttachCount();
    this.keepAlive = keepAlive;
    boolean SPECIAL_DURABLE =
        Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "SPECIAL_DURABLE");
    if (cnt > 0) {
      // special case to allow closing durable client pool under the keep alive flag
      // closing regions prior to closing pool can cause them to unregister interest
      if (SPECIAL_DURABLE) {
        synchronized (simpleLock) {
          try {
            if (cache == null && distributedSystem != null) {
              cache = distributedSystem.getCache();
              if (cache == null) {
                throw new IllegalStateException("Cache must be created before creating pool");
              }
            }
            if (!cache.isClosed() && getPoolOrCacheCancelInProgress() == null) {
              Set<Region<?, ?>> regions = cache.rootRegions();
              for (Region<?, ?> roots : regions) {
                Set<Region<?, ?>> subregions = roots.subregions(true);
                for (Region<?, ?> subroots : subregions) {
                  if (!subroots.isDestroyed() && subroots.getAttributes().getPoolName() != null
                      && subroots.getAttributes().getPoolName().equals(name)) {
                    if (logger.isDebugEnabled()) {
                      logger.debug(
                          "PoolImpl.destroy[ Region connected count:{} Region subroot closing:{} Pool Name:{} ]",
                          cnt, subroots.getName(), name);
                    }
                    subroots.close();
                  }
                }

                if (!roots.isDestroyed() && roots.getAttributes().getPoolName() != null
                    && roots.getAttributes().getPoolName().equals(name)) {
                  if (logger.isDebugEnabled()) {
                    logger.debug(
                        "PoolImpl.destroy[ Region connected count:{} Region root closing:{} Pool Name:{} ]",
                        cnt, roots.getName(), name);
                  }
                  roots.close();
                }
              }
            }
          } catch (Exception e) {
            if (logger.isDebugEnabled()) {
              logger.debug(e.getMessage(), e);
            }
          }
        }
      }

      cnt = getAttachCount();
      if (cnt > 0) {
        throw new IllegalStateException(
            String.format("Pool could not be destroyed because it is still in use by %s regions",
                cnt));
      }
    }
    if (pm.unregister(this)) {
      basicDestroy(keepAlive);
    }
  }

  /**
   * Destroys this pool but does not unregister it. This is used by the PoolManagerImpl when it
   * wants to close all its pools.
   */
  public synchronized void basicDestroy(boolean keepAlive) {
    if (!isDestroyed()) {
      destroyed = true;
      this.keepAlive = keepAlive;
      // LOG: changed from config to info
      logger.info("Destroying connection pool {}", name);

      try {
        if (backgroundProcessor != null) {
          backgroundProcessor.shutdown();
          if (!backgroundProcessor.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
            logger.warn("Timeout waiting for background tasks to complete.");
          }
        }
      } catch (RuntimeException e) {
        logger.error("Error encountered while stopping backgroundProcessor.", e);
      } catch (InterruptedException e) {
        logger.error("Interrupted while stopping backgroundProcessor", e);
      }

      try {
        if (source != null) {
          source.stop();
        }
      } catch (RuntimeException e) {
        logger.error("Error encountered while stopping connection source.", e);
      }

      try {
        if (queueManager != null) {
          queueManager.close(keepAlive);
        }
      } catch (RuntimeException e) {
        logger.error(
            "Error encountered while stopping subscription manager",
            e);
      }

      try {
        if (manager != null) {
          manager.close(keepAlive);
        }
      } catch (RuntimeException e) {
        logger.error("Error encountered while stopping connection manager.", e);
      }

      try {
        endpointManager.close();
      } catch (RuntimeException e) {
        logger.error("Error encountered while stopping endpoint manager", e);
      }

      try {
        if (stats != null) {
          stats.close();
        }
      } catch (RuntimeException e) {
        logger.error("Error while closing statistics", e);
      }
    }
  }

  @Override
  public boolean isDestroyed() {
    return destroyed;
  }


  private ConnectionSource getSourceImpl(LocatorDiscoveryCallback locatorDiscoveryCallback) {
    List<InetSocketAddress> locators = getLocators();
    if (locators.isEmpty()) {
      return new ExplicitConnectionSourceImpl(getServers());
    } else {
      AutoConnectionSourceImpl source = new AutoConnectionSourceImpl(locatorAddresses,
          getServerGroup(), socketConnectTimeout);
      if (locatorDiscoveryCallback != null) {
        source.setLocatorDiscoveryCallback(locatorDiscoveryCallback);
      }
      return source;
    }
  }

  /**
   * Used internally by xml parsing code.
   */
  public void sameAs(Object obj) {
    if (!(obj instanceof PoolImpl)) {
      throw new RuntimeException(String
          .format("%s is not the same as %s because it should have been a PoolImpl", this, obj));
    }
    PoolImpl other = (PoolImpl) obj;
    if (!getName().equals(other.getName())) {
      throw new RuntimeException(
          String.format("Pool %s are different", "names"));
    }
    if (getSocketConnectTimeout() != other.getSocketConnectTimeout()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "socketConnectimeout"));
    }
    if (getFreeConnectionTimeout() != other.getFreeConnectionTimeout()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "connectionTimeout"));
    }
    if (getLoadConditioningInterval() != other.getLoadConditioningInterval()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "connectionLifetime"));
    }
    if (getSocketBufferSize() != other.getSocketBufferSize()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "socketBufferSize"));
    }
    if (getThreadLocalConnections() != other.getThreadLocalConnections()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "threadLocalConnections"));
    }
    if (getReadTimeout() != other.getReadTimeout()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "readTimeout"));
    }
    if (getMinConnections() != other.getMinConnections()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "MinConnections"));
    }
    if (getMaxConnections() != other.getMaxConnections()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "MaxConnections"));
    }
    if (getRetryAttempts() != other.getRetryAttempts()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "RetryAttempts"));
    }
    if (getIdleTimeout() != other.getIdleTimeout()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "IdleTimeout"));
    }
    if (getPingInterval() != other.getPingInterval()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "PingInterval"));
    }
    if (getStatisticInterval() != other.getStatisticInterval()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "StatisticInterval"));
    }
    if (getSubscriptionAckInterval() != other.getSubscriptionAckInterval()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "subscriptionAckInterval"));
    }
    if (getSubscriptionEnabled() != other.getSubscriptionEnabled()) {
      throw new RuntimeException(
          String.format("Pool %s is different", "subscriptionEnabled"));
    }
    if (getSubscriptionMessageTrackingTimeout() != other.getSubscriptionMessageTrackingTimeout()) {
      throw new RuntimeException(String.format("Pool %s is different",
          "subscriptionMessageTrackingTimeout"));
    }
    if (getSubscriptionRedundancy() != other.getSubscriptionRedundancy()) {
      throw new RuntimeException(String.format("Pool %s is different",
          "subscriptionRedundancyLevel"));
    }
    if (!getServerGroup().equals(other.getServerGroup())) {
      throw new RuntimeException(
          String.format("Pool %s is different", "serverGroup"));
    }
    if (!getLocators().equals(other.getLocators())) {
      throw new RuntimeException(
          String.format("Pool %s are different", "locators"));
    }
    if (!getServers().equals(other.getServers())) {
      throw new RuntimeException(
          String.format("Pool %s are different", "servers"));
    }
    // ignore startDisabled
  }

  @Override
  public PoolStats getStats() {
    return stats;
  }


  /**
   * Execute the given op on the servers that this pool connects to. This method is responsible for
   * retrying the op if an attempt fails. It will only execute it once and on one server.
   *
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   * @since GemFire 5.7
   */
  @Override
  public Object execute(Op op) {
    // if(multiuser)
    // get a server from threadlocal cache else throw cacheWriterException
    // executeOn(ServerLocation server, Op op, boolean accessed,boolean onlyUseExistingCnx)

    // Retries are ignored here. FIX IT - FIXED.
    // But this may lead to a user getting authenticated on all servers, even if
    // a single server could have serviced all its requests.
    authenticateIfRequired(null, op);
    return executor.execute(op);
  }

  /**
   * Execute the given op on the servers that this pool connects to. This method is responsible for
   * retrying the op if an attempt fails. It will only execute it once and on one server.
   *
   * @param op the operation to execute
   * @param retries how many times to retry the operation
   * @return the result of execution if any; null if not
   * @since GemFire 5.7
   */
  @Override
  public Object execute(Op op, int retries) {
    authenticateIfRequired(null, op);
    return executor.execute(op, retries);
  }

  /**
   * Execute the given op on the given server.
   *
   * @param server the server to do the execution on
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  @Override
  public Object executeOn(ServerLocation server, Op op) {
    authenticateIfRequired(server, op);
    return executor.executeOn(server, op);
  }

  /**
   * Execute the given op on the given server.
   *
   * @param server the server to do the execution on
   * @param op the operation to execute
   * @param accessed true if the connection is accessed by this execute
   * @return the result of execution if any; null if not
   */
  @Override
  public Object executeOn(ServerLocation server, Op op, boolean accessed,
      boolean onlyUseExistingCnx) {
    authenticateIfRequired(server, op);
    return executor.executeOn(server, op, accessed, onlyUseExistingCnx);
  }

  /**
   * Execute the given op on the given connection.
   *
   * @param con the connection to do the execution on
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  @Override
  public Object executeOn(Connection con, Op op) {
    authenticateIfRequired(con.getServer(), op);
    return executor.executeOn(con, op);
  }

  @Override
  public Object executeOn(Connection con, Op op, boolean timeoutFatal) {
    return executor.executeOn(con, op, timeoutFatal);
  }

  /**
   * Execute the given op on all the servers that have server-to-client queues for this pool
   *
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   * @since GemFire 5.7
   */
  @Override
  public Object executeOnQueuesAndReturnPrimaryResult(Op op) {
    authenticateOnAllServers(op);
    return executor.executeOnQueuesAndReturnPrimaryResult(op);
  }

  @Override
  public void executeOnAllQueueServers(Op op)
      throws NoSubscriptionServersAvailableException, SubscriptionNotEnabledException {
    authenticateOnAllServers(op);
    executor.executeOnAllQueueServers(op);
  }

  /**
   * Execute the given op on the current primary server.
   *
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  @Override
  public Object executeOnPrimary(Op op) {
    return executor.executeOnPrimary(op);
  }

  @Override
  public Map<ServerLocation, Endpoint> getEndpointMap() {
    return endpointManager.getEndpointMap();
  }

  @Override
  public ScheduledExecutorService getBackgroundProcessor() {
    return backgroundProcessor;
  }

  @Override
  public RegisterInterestTracker getRITracker() {
    return riTracker;
  }

  /**
   * Test hook that returns the number of servers we currently have connections to.
   */
  public int getConnectedServerCount() {
    return endpointManager.getConnectedServerCount();
  }

  /**
   * Test hook. Verify if this EventId is already present in the map or not. If it is already
   * present then return true.
   *
   * @param eventId the EventId of the incoming event
   * @return true if it is already present
   * @since GemFire 5.1
   */
  public boolean verifyIfDuplicate(EventID eventId) {
    return queueManager.getState().verifyIfDuplicate(eventId);
  }

  /**
   * Borrows a connection from the pool.. Used by gateway and tests. Any connection that is acquired
   * using this method must be returned using returnConnection, even if it is destroyed.
   *
   */
  public Connection acquireConnection() {
    return manager.borrowConnection(45000L);
  }

  /**
   * Hook to return connections that were acquired using acquireConnection.
   */
  public void returnConnection(Connection conn) {
    manager.returnConnection(conn);
  }

  /**
   * Borrows a connection to a specific server from the pool.. Used by gateway and tests. Any
   * connection
   * that is acquired using this method must be returned using returnConnection, even if it is
   * destroyed.
   *
   */
  public Connection acquireConnection(ServerLocation loc) {
    return manager.borrowConnection(loc, 15000L, false);
  }

  /**
   * Test hook that returns an unnmodifiable list of the current denylisted servers
   */
  public Set getDenylistedServers() {
    return connectionFactory.getDenyList().getBadServers();
  }

  /**
   * Test hook to handle an exception that happened on the given connection
   */
  public void processException(Throwable e, Connection con) {
    executor.handleException(e, con, 0, false);
  }

  /**
   * Test hook that returns the ThreadIdToSequenceIdMap
   */
  public Map getThreadIdToSequenceIdMap() {
    if (queueManager == null)
      return Collections.emptyMap();
    if (queueManager.getState() == null)
      return Collections.emptyMap();
    return queueManager.getState().getThreadIdToSequenceIdMap();
  }

  /**
   * Test hook that returns true if we have a primary and its updater thread is alive.
   */
  public boolean isPrimaryUpdaterAlive() {
    return ((QueueManagerImpl) queueManager).isPrimaryUpdaterAlive();
  }

  /**
   * Test hook used to simulate a kill of the primaryEndpoint
   */
  public void killPrimaryEndpoint() // throws ServerException
  {
    boolean ok = false;
    if (queueManager != null) {
      QueueManager.QueueConnections cons = queueManager.getAllConnections();
      Connection con = cons.getPrimary();
      if (con != null) {
        final String msg = "killing primary endpoint";
        logger.info("<ExpectedException action=add>{}</ExpectedException>", msg);
        Exception e = new Exception(msg);
        try {
          processException(e, con);
        } catch (ServerConnectivityException ignore) {
        } finally {
          logger.info("<ExpectedException action=remove>{}</ExpectedException>", msg);
        }
        // do some validation here that we are no longer connected to "sl"
        ok = true;
      }
    }
    if (!ok) {
      throw new IllegalStateException("primaryEndpoint was null");
    }
  }

  // used by unit tests to confirm if readyForEvents has been called on a pool
  private boolean readyForEventsCalled;

  public boolean getReadyForEventsCalled() {
    return readyForEventsCalled;
  }

  public void readyForEvents(InternalDistributedSystem system) {
    if (!isDurableClient() || queueManager == null) {
      return;
    }
    readyForEventsCalled = true;
    queueManager.readyForEvents(system);

  }

  @Override
  public boolean isDurableClient() {
    DistributionConfig config = distributedSystem.getConfig();
    String durableClientId = config.getDurableClientId();
    return durableClientId != null && durableClientId.length() > 0;
  }

  /**
   * Test hook that returns a string consisting of the host name and port of the primary server.
   * Null is returned if we have no primary.
   */
  public String getPrimaryName() {
    String result = null;
    ServerLocation sl = getPrimary();
    if (sl != null) {
      result = sl.getHostName() + sl.getPort();
    }
    return result;
  }

  @Override
  @VisibleForTesting
  public int getPrimaryPort() {
    int result = -1;
    ServerLocation sl = getPrimary();
    if (sl != null) {
      result = sl.getPort();
    }
    return result;
  }

  /**
   * Test hook that returns a string consisting of the host name and port of the primary server.
   * Null is returned if we have no primary.
   */
  public ServerLocation getPrimary() {
    ServerLocation result = null;
    if (queueManager != null) {
      QueueManager.QueueConnections cons = queueManager.getAllConnections();
      Connection con = cons.getPrimary();
      result = con.getServer();
    }
    return result;
  }

  /**
   * Test hook to get a connection to the primary server.
   */
  public Connection getPrimaryConnection() {
    if (queueManager != null) {
      QueueManager.QueueConnections cons = queueManager.getAllConnections();
      return cons.getPrimary();
    }
    return null;
  }

  /**
   * Test hook that returns a list of strings. Each string consists of the host name and port of a
   * redundant server. An empty list is returned if we have no redundant servers.
   */
  public List<String> getRedundantNames() {
    List<String> result = Collections.emptyList();
    if (queueManager != null) {
      QueueManager.QueueConnections cons = queueManager.getAllConnections();
      List<Connection> backupCons = cons.getBackups();
      if (backupCons.size() > 0) {
        result = new ArrayList<>(backupCons.size());
        for (Connection con : backupCons) {
          ServerLocation sl = con.getServer();
          result.add(sl.getHostName() + sl.getPort());
        }
      }
    }
    return result;
  }

  /**
   * Test hook that returns a list of ServerLocation instances. Each ServerLocation describes a
   * redundant server. An empty list is returned if we have no redundant servers.
   */
  public List<ServerLocation> getRedundants() {
    List<ServerLocation> result = Collections.emptyList();
    if (queueManager != null) {
      QueueManager.QueueConnections cons = queueManager.getAllConnections();
      List<Connection> backupCons = cons.getBackups();
      if (backupCons.size() > 0) {
        result = new ArrayList<>(backupCons.size());
        for (Connection con : backupCons) {
          result.add(con.getServer());
        }
      }
    }
    return result;
  }

  /**
   * Test hook to find out current number of connections this pool has.
   */
  public int getConnectionCount() {
    return manager.getConnectionCount();
  }

  /**
   * Atomic counter used to keep track of services using this pool.
   *
   * @since GemFire 5.7
   */
  @MakeNotStatic
  private final AtomicInteger attachCount = new AtomicInteger();

  @MutableForTesting
  public static volatile boolean IS_INSTANTIATOR_CALLBACK = false;

  /**
   * Returns number of services currently using/attached to this pool.
   * <p>
   * Made public so it can be used by tests
   *
   * @since GemFire 5.7
   */
  public int getAttachCount() {
    return attachCount.get();
  }

  /**
   * This needs to be called when a service (like a Region or CQService) starts using a pool.
   *
   * @since GemFire 5.7
   */
  public void attach() {
    attachCount.getAndIncrement();
  }

  /**
   * This needs to be called when a service (like a Region or CQService) stops using a pool.
   *
   * @since GemFire 5.7
   */
  @Override
  public void detach() {
    attachCount.getAndDecrement();
  }

  /**
   * Returns a list of ServerLocation instances; one for each server we are currently connected to.
   */
  public List<ServerLocation> getCurrentServers() {
    Map<ServerLocation, Endpoint> endpointMap = endpointManager.getEndpointMap();
    return new ArrayList<>(endpointMap.keySet());
  }

  /**
   * Test hook that returns a list of server names (host+port); one for each server we are currently
   * connected to.
   */
  public List<String> getCurrentServerNames() {
    List<ServerLocation> servers = getCurrentServers();
    ArrayList<String> result = new ArrayList<>(servers.size());
    for (ServerLocation sl : servers) {
      String name = sl.getHostName() + sl.getPort();
      result.add(name);
    }
    return result;
  }

  @Override
  public EndpointManager getEndpointManager() {
    return endpointManager;
  }

  /**
   * Fetch the connection source for this pool
   *
   * @return the source
   */
  public ConnectionSource getConnectionSource() {
    return source;
  }

  private static void setTEST_DURABLE_IS_NET_DOWN(boolean v) {
    TEST_DURABLE_IS_NET_DOWN = v;
  }

  /**
   * test hook
   */
  public void endpointsNetDownForDUnitTest() {
    logger.debug("PoolImpl - endpointsNetDownForDUnitTest");
    setTEST_DURABLE_IS_NET_DOWN(true);
    try {
      Thread.sleep(pingInterval * 2);
    } catch (java.lang.InterruptedException ignore) {
      // do nothing.
    }

    Map<ServerLocation, Endpoint> endpoints = endpointManager.getEndpointMap();
    for (Endpoint endpoint : endpoints.values()) {
      logger.debug("PoolImpl Simulating crash of endpoint {}", endpoint);
      endpointManager.serverCrashed(endpoint);
    }
  }

  /**
   * test hook
   */
  public void endpointsNetUpForDUnitTest() {
    setTEST_DURABLE_IS_NET_DOWN(false);
    try {
      Thread.sleep(pingInterval * 2);
    } catch (java.lang.InterruptedException ignore) {
      // do nothing.
    }
  }

  /**
   * test hook
   */
  public int getInvalidateCount() {
    return ((QueueStateImpl) queueManager.getState()).getInvalidateCount();
  }

  @Override
  public ServerLocation getServerAffinityLocation() {
    return executor.getServerAffinityLocation();
  }

  @Override
  public void setServerAffinityLocation(ServerLocation serverLocation) {
    executor.setServerAffinityLocation(serverLocation);
  }

  public ServerLocation getNextOpServerLocation() {
    return executor.getNextOpServerLocation();
  }

  /**
   * Test hook for getting the client proxy membership id from this proxy.
   */
  public ClientProxyMembershipID getProxyID() {
    return proxyId;
  }


  public void emergencyClose() {
    destroyed = true;
    manager.emergencyClose();
    queueManager.emergencyClose();
  }

  ///////////////////// start test hooks ///////////////////////
  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  @MutableForTesting
  public static volatile boolean AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;

  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  @MutableForTesting
  public static volatile boolean BEFORE_REGISTER_CALLBACK_FLAG = false;

  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  @MutableForTesting
  public static volatile boolean BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = false;

  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  @MutableForTesting
  public static volatile boolean AFTER_REGISTER_CALLBACK_FLAG = false;

  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  @MutableForTesting
  public static volatile boolean BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;

  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  @MutableForTesting
  public static volatile boolean BEFORE_SENDING_CLIENT_ACK_CALLBACK_FLAG = false;
  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  @MutableForTesting
  public static volatile boolean AFTER_QUEUE_DESTROY_MESSAGE_FLAG = false;

  /**
   * Test hook flag to notify observer(s) that a primary is recovered either from a backup or from a
   * new connection.
   */
  @MutableForTesting
  static volatile boolean AFTER_PRIMARY_RECOVERED_CALLBACK_FLAG = false;

  public abstract static class PoolTask implements Runnable {

    @Override
    public void run() {
      try {
        run2();
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (CancelException ignore) {
        if (logger.isDebugEnabled()) {
          logger.debug("Pool task <{}> cancelled", this);
        }
      } catch (Throwable t) {
        logger.error(String.format("Unexpected error in pool task <%s>", this), t);
      }

    }

    public abstract void run2();
  }

  ///////////////////// end test hooks ///////////////////////

  protected class PublishClientStatsTask extends PoolTask {
    @Override
    public void run2() {
      ClientStatsManager.publishClientStats(PoolImpl.this);
    }
  }

  /**
   * A cancel criterion that checks both the pool and the cache for canceled status.
   */
  protected class PoolOrCacheStopper extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      return getPoolOrCacheCancelInProgress();
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      return generatePoolOrCacheCancelledException(e);
    }

  }

  /**
   * A cancel criterion that checks only if this pool has been closed. This is necessary because
   * there are some things that we want to allow even after the cache has started closing.
   */
  protected class Stopper extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      if (destroyed) {
        return "Pool " + PoolImpl.this + " is shut down";
      } else {
        return null;
      }
    }

    @Override
    public RuntimeException generateCancelledException(Throwable t) {
      String reason = cancelInProgress();
      if (reason == null) {
        return null;
      }
      return new PoolCancelledException(reason, t);
    }
  }

  /**
   * Returns the QueryService, that can be used to execute Query functions on the servers associated
   * with this pool.
   *
   * @return the QueryService
   */
  @Override
  public QueryService getQueryService() {
    DefaultQueryService queryService = new DefaultQueryService(cache);
    queryService.setPool(this);
    return queryService;
  }

  public RegionService createAuthenticatedCacheView(Properties properties) {
    if (!multiuserSecureModeEnabled) {
      throw new UnsupportedOperationException(
          "Operation not supported when multiuser-authentication is false.");
    }
    if (properties == null || properties.isEmpty()) {
      throw new IllegalArgumentException("Security properties cannot be empty.");
    }

    Properties props = new Properties();
    for (Entry<Object, Object> entry : properties.entrySet()) {
      props.setProperty((String) entry.getKey(), (String) entry.getValue());
    }
    if (cache == null && distributedSystem != null) {
      cache = distributedSystem.getCache();
      if (cache == null) {
        throw new IllegalStateException(
            "Cache must be created before creating pool");
      }
    }
    ProxyCache proxy = new ProxyCache(props, cache, this);
    synchronized (proxyCacheList) {
      proxyCacheList.add(proxy);
    }
    return proxy;
  }

  private volatile CancelCriterion cacheCriterion = null;

  private RuntimeException generatePoolOrCacheCancelledException(Throwable e) {
    RuntimeException re = getCancelCriterion().generateCancelledException(e);
    if (re != null) {
      return re;
    }
    if (cache == null) {
      if (cacheCriterion != null) {
        return cacheCriterion.generateCancelledException(e);
      }
      if (distributedSystem != null) {
        cache = distributedSystem.getCache();
        if (cache == null) {
          throw new IllegalStateException(
              "Cache must be created before creating pool");
        }
      }
    } else {
      if (cacheCriterion == null || cacheCriterion != cache.getCancelCriterion()) {
        cacheCriterion = cache.getCancelCriterion();
      }
      return cacheCriterion.generateCancelledException(e);
    }
    return null;
  }

  @Override
  public String getPoolOrCacheCancelInProgress() {
    String reason;
    try {
      reason = getCancelCriterion().cancelInProgress();
      if (reason != null) {
        return reason;
      }
      if (cache == null) {
        if (cacheCriterion != null) {
          return cacheCriterion.cancelInProgress();
        }
        return null;
      } else {
        if (cacheCriterion == null) {
          cacheCriterion = cache.getCancelCriterion();
        } else if (cacheCriterion != cache.getCancelCriterion()) {
          /*
           * If the cache instance has somehow changed, we need to get a reference to the new
           * criterion. This is pretty unlikely because the cache closes all the pools when it shuts
           * down, but I wanted to be safe.
           */
          cacheCriterion = cache.getCancelCriterion();
        }
        return cacheCriterion.cancelInProgress();
      }
    } catch (CancelException cce) {
      if (cce.getMessage() != null) {
        return cce.getMessage();
      } else {
        return "cache is closed";
      }
    }
  }

  @Override
  public boolean getKeepAlive() {
    if (cache == null) {
      return keepAlive;
    }

    return cache.keepDurableSubscriptionsAlive();
  }

  ArrayList<ProxyCache> getProxyCacheList() {
    return proxyCacheList;
  }

  /**
   * This is only for multi-user case
   *
   * Assert thread-local var is not null.
   *
   * If serverLocation is non-null, check if the the user is authenticated on that server. If not,
   * authenticate it and return.
   *
   */
  private void authenticateIfRequired(ServerLocation serverLocation, Op op) {
    if (!multiuserSecureModeEnabled) {
      return;
    }

    if (!(op instanceof AbstractOp) || !((AbstractOp) op).needsUserId()) {
      return;
    }

    UserAttributes userAttributes = UserAttributes.userAttributes.get();
    if (userAttributes == null) {
      throw new UnsupportedOperationException(
          "Use Pool APIs for doing operations when multiuser-secure-mode-enabled is set to true.");
    }

    if (serverLocation != null) {
      if (!userAttributes.getServerToId().containsKey(serverLocation)) {
        Long userId = (Long) AuthenticateUserOp.executeOn(serverLocation, this,
            userAttributes.getCredentials());
        if (userId != null) {
          userAttributes.setServerToId(serverLocation, userId);
        }
      }
    }
  }

  private void authenticateOnAllServers(Op op) {
    if (multiuserSecureModeEnabled && ((AbstractOp) op).needsUserId()) {
      UserAttributes userAttributes = UserAttributes.userAttributes.get();
      if (userAttributes != null) {
        ConcurrentHashMap<ServerLocation, Long> map = userAttributes.getServerToId();

        if (queueManager == null) {
          throw new SubscriptionNotEnabledException();
        }
        Connection primary = queueManager.getAllConnectionsNoWait().getPrimary();
        if (primary != null && !map.containsKey(primary.getServer())) {
          Long userId = (Long) AuthenticateUserOp.executeOn(primary.getServer(), this,
              userAttributes.getCredentials());
          if (userId != null) {
            map.put(primary.getServer(), userId);
          }
        }

        List<Connection> backups = queueManager.getAllConnectionsNoWait().getBackups();
        for (Connection conn : backups) {
          if (!map.containsKey(conn.getServer())) {
            Long userId = (Long) AuthenticateUserOp.executeOn(conn.getServer(), this,
                userAttributes.getCredentials());
            if (userId != null) {
              map.put(conn.getServer(), userId);
            }
          }
        }
      } else {
        throw new UnsupportedOperationException(
            "Use Pool APIs for doing operations when multiuser-secure-mode-enabled is set to true.");
      }
    }
  }

  public void setPendingEventCount(int count) {
    primaryQueueSize.set(count);
  }

  @Override
  public int getPendingEventCount() {
    if (!isDurableClient() || queueManager == null) {
      throw new IllegalStateException(
          "Only durable clients should call getPendingEventCount()");
    }
    if (readyForEventsCalled) {
      throw new IllegalStateException(
          "getPendingEventCount() should be called before invoking readyForEvents().");
    }
    return primaryQueueSize.get();
  }

  @Override
  public int getSubscriptionTimeoutMultiplier() {
    return subscriptionTimeoutMultiplier;
  }

  public int calculateRetryAttempts(Throwable cause) {

    int maxRetryAttempts = getRetryAttempts();

    if (maxRetryAttempts == PoolFactory.DEFAULT_RETRY_ATTEMPTS) {
      // If the retryAttempt is set to default(-1). Try executing on all servers once.
      // As calculating number of servers involves sending message to locator, it is
      // done only when there is an exception.
      if (cause instanceof ServerConnectivityException
          && cause.getMessage().contains(ConnectionManagerImpl.SOCKET_TIME_OUT_MSG)) {
        // The request was sent once.
        maxRetryAttempts = getConnectionSource().getAllServers().size() - 1;
      } else {
        // The client was unable to establish a connection before sending the
        // request.
        maxRetryAttempts = getConnectionSource().getAllServers().size();
      }
    }

    return maxRetryAttempts;
  }

}
