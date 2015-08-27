/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.NoSubscriptionServersAvailableException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionService;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.SubscriptionNotEnabledException;
import com.gemstone.gemfire.cache.client.internal.pooling.ConnectionManager;
import com.gemstone.gemfire.cache.client.internal.pooling.ConnectionManagerImpl;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.PoolCancelledException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.DummyStatisticsFactory;
import com.gemstone.gemfire.internal.ScheduledThreadPoolExecutorWithKeepAlive;
import com.gemstone.gemfire.internal.admin.ClientStatsManager;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.internal.cache.PoolManagerImpl;
import com.gemstone.gemfire.internal.cache.PoolStats;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * Manages the client side of client to server connections
 * and client queues. 
 * 
 * @author dsmith
 * @since 5.7
 */
public class PoolImpl implements InternalPool {
  private static final Logger logger = LogService.getLogger();
  
  public static final int HANDSHAKE_TIMEOUT = Long.getLong("gemfire.PoolImpl.HANDSHAKE_TIMEOUT", AcceptorImpl.DEFAULT_HANDSHAKE_TIMEOUT_MS).intValue();
  public static final long SHUTDOWN_TIMEOUT = Long.getLong("gemfire.PoolImpl.SHUTDOWN_TIMEOUT", 30000).longValue();
  public static final int BACKGROUND_TASK_POOL_SIZE = Integer.getInteger("gemfire.PoolImpl.BACKGROUND_TASK_POOL_SIZE", 20).intValue();
  public static final int BACKGROUND_TASK_POOL_KEEP_ALIVE = Integer.getInteger("gemfire.PoolImpl.BACKGROUND_TASK_POOL_KEEP_ALIVE", 1000).intValue();
  //For durable client tests only. Connection Sources read this flag
  //and return an empty list of servers.
  public volatile static boolean TEST_DURABLE_IS_NET_DOWN = false;

  private final String name;
  private final int freeConnectionTimeout;
  private final int loadConditioningInterval;
  private final int socketBufferSize;
  private final boolean threadLocalConnections;
  private final int readTimeout;
  private final boolean subscriptionEnabled;
  private final boolean prSingleHopEnabled;
  private final int subscriptionRedundancyLevel;
  private final int subscriptionMessageTrackingTimeout;
  private final int subscriptionAckInterval;
  private final String serverGroup;
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
  private final InternalDistributedSystem dsys; 

  private final ClientProxyMembershipID proxyId;
  protected final CancelCriterion cancelCriterion;
  private final ConnectionFactoryImpl connectionFactory;

  private final ArrayList<ProxyCache> proxyCacheList;
  
  private final GatewaySender gatewaySender;
  
  private boolean keepAlive=false;
  private static Object simpleLock=new Object();

  public static final int PRIMARY_QUEUE_NOT_AVAILABLE = -2;
  public static final int PRIMARY_QUEUE_TIMED_OUT = -1;
  private AtomicInteger primaryQueueSize = new AtomicInteger(PRIMARY_QUEUE_NOT_AVAILABLE);

  public static PoolImpl create(PoolManagerImpl pm, String name, Pool attributes) {
    PoolImpl pool = new PoolImpl(pm, name, attributes);
    pool.finishCreate(pm);
    return pool;
  }
  
  public boolean isUsedByGateway() {
    return usedByGateway;
  }

  /**
   * @since 5.7
   */
  protected void finishCreate(PoolManagerImpl pm) {
    pm.register(this);
    try {
      start();
    } catch(RuntimeException e) {
      try {
        destroy(false);
      } catch(RuntimeException e2) {
        //do nothing
      }
      throw e;
    }
  }

  protected PoolImpl(PoolManagerImpl pm, String name, Pool attributes) {
  	this.pm = pm;
    this.name = name;
    this.freeConnectionTimeout = attributes.getFreeConnectionTimeout();
    this.loadConditioningInterval = attributes.getLoadConditioningInterval();
    this.socketBufferSize = attributes.getSocketBufferSize();
    this.threadLocalConnections = attributes.getThreadLocalConnections();
    this.readTimeout = attributes.getReadTimeout();
    this.minConnections = attributes.getMinConnections();
    this.maxConnections = attributes.getMaxConnections();
    this.retryAttempts = attributes.getRetryAttempts();
    this.idleTimeout = attributes.getIdleTimeout();
    this.pingInterval = attributes.getPingInterval();
    this.statisticInterval = attributes.getStatisticInterval();
    this.subscriptionEnabled = attributes.getSubscriptionEnabled();
    this.prSingleHopEnabled = attributes.getPRSingleHopEnabled();
    this.subscriptionRedundancyLevel = attributes.getSubscriptionRedundancy();
    this.subscriptionMessageTrackingTimeout = attributes.getSubscriptionMessageTrackingTimeout();
    this.subscriptionAckInterval = attributes.getSubscriptionAckInterval();
    this.serverGroup = attributes.getServerGroup();
    this.multiuserSecureModeEnabled = attributes.getMultiuserAuthentication();
    this.locators = attributes.getLocators();
    this.servers = attributes.getServers();
    this.startDisabled = ((PoolFactoryImpl.PoolAttributes)attributes).startDisabled
      || !pm.isNormal();
    this.usedByGateway = ((PoolFactoryImpl.PoolAttributes)attributes).isGateway();
    this.gatewaySender = ((PoolFactoryImpl.PoolAttributes)attributes).getGatewaySender();
//    if (this.subscriptionEnabled && this.multiuserSecureModeEnabled) {
//      throw new IllegalStateException(
//          "subscription-enabled and multiuser-authentication both cannot be true.");
//    }
    InternalDistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    if(ds==null) {
      throw new IllegalStateException(LocalizedStrings.PoolImpl_DISTRIBUTED_SYSTEM_MUST_BE_CREATED_BEFORE_CREATING_POOL.toLocalizedString());
    }
    this.securityLogWriter = ds.getSecurityInternalLogWriter();
    if (!ds.getConfig().getStatisticSamplingEnabled()
        && this.statisticInterval > 0) {
      logger.info(LocalizedMessage.create(
              LocalizedStrings.PoolImpl_STATISTIC_SAMPLING_MUST_BE_ENABLED_FOR_SAMPLING_RATE_OF_0_TO_TAKE_AFFECT,
              this.statisticInterval));
    }
    this.dsys = ds;
    this.cancelCriterion = new Stopper();
    if(Boolean.getBoolean("gemfire.SPECIAL_DURABLE")) {
	    ClientProxyMembershipID.setPoolName(name);
	    this.proxyId = ClientProxyMembershipID.getNewProxyMembership(ds);
	    ClientProxyMembershipID.setPoolName(null);
    } else {
    	this.proxyId = ClientProxyMembershipID.getNewProxyMembership(ds);
    }
    StatisticsFactory statFactory = null;
    if(this.gatewaySender != null){
      statFactory = new DummyStatisticsFactory();
    }else{
      statFactory = ds;
    }
    this.stats = this.startDisabled
      ? null
      : new PoolStats(statFactory, getName()+"->"+(serverGroup==null || serverGroup.equals("") ? "[any servers]" : "["+getServerGroup()+"]"));
    
    source = getSourceImpl(((PoolFactoryImpl.PoolAttributes)attributes).locatorCallback);
    endpointManager = new EndpointManagerImpl(name, ds,this.cancelCriterion, this.stats);
    connectionFactory = new ConnectionFactoryImpl(source, endpointManager, ds,
        socketBufferSize, HANDSHAKE_TIMEOUT, readTimeout, proxyId, this.cancelCriterion,
        usedByGateway,gatewaySender, pingInterval, multiuserSecureModeEnabled, this);
    if(subscriptionEnabled) {
      queueManager = new QueueManagerImpl(this, endpointManager, source,
          connectionFactory, subscriptionRedundancyLevel, pingInterval, securityLogWriter,
          proxyId);
    }
    
    manager = new ConnectionManagerImpl(name, connectionFactory, endpointManager,
                                        maxConnections, minConnections,
                                        idleTimeout, loadConditioningInterval,
                                        securityLogWriter, pingInterval,
                                        cancelCriterion, getStats());
    //Fix for 43468 - make sure we check the cache cancel criterion if we get 
    //an exception, by passing in the poolOrCache stopper
    executor = new OpExecutorImpl(manager, queueManager, endpointManager,
        riTracker, retryAttempts, freeConnectionTimeout, threadLocalConnections,
        new PoolOrCacheStopper(), this);
    if (this.multiuserSecureModeEnabled) {
      this.proxyCacheList = new ArrayList<ProxyCache>();
    } else {
      this.proxyCacheList = null;
    }
  }

  /**
   * Return true if the given Pool is compatible with these attributes.
   * Currently this does what equals would but in the future we might
   * decide to weaken the compatibility contract.
   * @since 6.5
   */
  public boolean isCompatible(Pool p) {
    if (p == null) return false;
    return getFreeConnectionTimeout() ==            p.getFreeConnectionTimeout()
      && getLoadConditioningInterval() ==           p.getLoadConditioningInterval()
      && getSocketBufferSize() ==                   p.getSocketBufferSize()
      && getMinConnections() ==                     p.getMinConnections()
      && getMaxConnections() ==                     p.getMaxConnections()
      && getIdleTimeout() ==                        p.getIdleTimeout()
      && getPingInterval() ==                       p.getPingInterval()
      && getStatisticInterval() ==                  p.getStatisticInterval()
      && getRetryAttempts() ==                      p.getRetryAttempts()
      && getThreadLocalConnections() ==             p.getThreadLocalConnections()
      && getReadTimeout() ==                        p.getReadTimeout()
      && getSubscriptionEnabled() ==                p.getSubscriptionEnabled()
      && getPRSingleHopEnabled() ==                 p.getPRSingleHopEnabled()
      && getSubscriptionRedundancy() ==             p.getSubscriptionRedundancy()
      && getSubscriptionMessageTrackingTimeout() == p.getSubscriptionMessageTrackingTimeout()
      && getSubscriptionAckInterval() ==            p.getSubscriptionAckInterval()
      && getServerGroup().equals(                   p.getServerGroup())
      && getMultiuserAuthentication() ==         p.getMultiuserAuthentication()
      && getLocators().equals(                      p.getLocators())
      && getServers().equals(                       p.getServers());
  }
  
  private void start() {
    if (this.startDisabled) return;
    
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if(isDebugEnabled) {
      List locators = getLocators();
      if(!locators.isEmpty()) {
        logger.debug("PoolImpl - starting pool with locators: {}", locators);
      } else {
        logger.debug("PoolImpl -starting pool with servers: {}", getServers());
      }
    }
    
    final String timerName = "poolTimer-" + getName() + "-";
    backgroundProcessor = new ScheduledThreadPoolExecutorWithKeepAlive(
        BACKGROUND_TASK_POOL_SIZE, BACKGROUND_TASK_POOL_KEEP_ALIVE,
        TimeUnit.MILLISECONDS, new ThreadFactory() {
      AtomicInteger threadNum = new AtomicInteger();
      public Thread newThread(final Runnable r) {
        Thread result = new Thread(r, timerName + threadNum.incrementAndGet());
        result.setDaemon(true);
        return result;
      }
    });
    ((ScheduledThreadPoolExecutorWithKeepAlive) backgroundProcessor)
        .setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    ((ScheduledThreadPoolExecutorWithKeepAlive) backgroundProcessor)
    .setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    
    source.start(this);
    connectionFactory.start(backgroundProcessor);
    endpointManager.addListener(new InstantiatorRecoveryListener(backgroundProcessor, this));
    endpointManager.addListener(new DataSerializerRecoveryListener(backgroundProcessor, this));
    if(Boolean.getBoolean("gemfire.ON_DISCONNECT_CLEAR_PDXTYPEIDS"))
      endpointManager.addListener(new PdxRegistryRecoveryListener(this));
    endpointManager.addListener(new LiveServerPinger(this));
    
    manager.start(backgroundProcessor);
    if(queueManager != null) {
      if (isDebugEnabled) {
        logger.debug("starting queueManager");
      }
      queueManager.start(backgroundProcessor);
    }
    if (isDebugEnabled) {
      logger.debug("scheduling pings every {} milliseconds", pingInterval);
    }
    

    if (this.statisticInterval > 0 && this.dsys.getConfig().getStatisticSamplingEnabled()) {
      backgroundProcessor.scheduleWithFixedDelay(new PublishClientStatsTask(), statisticInterval, statisticInterval, TimeUnit.MILLISECONDS);
    }
    // LOG: changed from config to info
    logger.info(LocalizedMessage.create(
            LocalizedStrings.PoolImpl_POOL_0_STARTED_WITH_MULTIUSER_SECURE_MODE_ENABLED_1,
            new Object[] {this.name, this.multiuserSecureModeEnabled}));
  }
  
  /**
   * Returns the cancellation criterion for this proxy
   * @return the cancellation criterion
   */
  public CancelCriterion getCancelCriterion() {
    return this.cancelCriterion;
  }

  public void releaseThreadLocalConnection() {
    executor.releaseThreadLocalConnection();
  }
  
  public void setupServerAffinity(boolean allowFailover) {
    executor.setupServerAffinity(allowFailover);
  }
  
  public void releaseServerAffinity() {
    executor.releaseServerAffinity();
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.Pool#getName()
   */
  public String getName() {
    return this.name;
  }
  public int getFreeConnectionTimeout() {
    return this.freeConnectionTimeout;
  }
  public int getLoadConditioningInterval() {
    return this.loadConditioningInterval;
  }
  public int getMaxConnections() {
    return maxConnections;
  }
  public int getMinConnections() {
    return minConnections;
  }
  public int getRetryAttempts() {
    return retryAttempts;
  }
  public long getIdleTimeout() {
    return idleTimeout;
  }
  public long getPingInterval() {
    return pingInterval;
  }
  public int getStatisticInterval() {
    return this.statisticInterval;
  }
  public int getSocketBufferSize() {
    return this.socketBufferSize;
  }
  public boolean getThreadLocalConnections() {
    return this.threadLocalConnections;
  }
  public int getReadTimeout() {
    return this.readTimeout;
  }
  public boolean getSubscriptionEnabled() {
    return this.subscriptionEnabled;
  }
  
  public boolean getPRSingleHopEnabled() {
    return this.prSingleHopEnabled;
  }
  
  public int getSubscriptionRedundancy() {
    return this.subscriptionRedundancyLevel;
  }
  public int getSubscriptionMessageTrackingTimeout() {
    return this.subscriptionMessageTrackingTimeout;
  }
  public int getSubscriptionAckInterval() {
    return subscriptionAckInterval;
  }
  public String getServerGroup() {
    return this.serverGroup;
  }

  public boolean getMultiuserAuthentication() {
    return this.multiuserSecureModeEnabled;
  }

  public List<InetSocketAddress> getLocators() {
    return this.locators;
  }
  public List<InetSocketAddress> getServers() {
    return this.servers;
  }

  public GatewaySender getGatewaySender() {
    return gatewaySender;
  }
  
  public InternalLogWriter getSecurityInternalLogWriter() {
    return this.securityLogWriter;
  }
  
  public void destroy() {
    destroy(false);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(100);
    sb.append(this.getClass().getSimpleName()).append('@')
        .append(System.identityHashCode(this)).append(" name=")
        .append(getName());
    return sb.toString();
  }

  public boolean getKeepAlive(){
      return this.keepAlive;
  }
  
  public void destroy(boolean keepAlive) {
    int cnt = getAttachCount();
    this.keepAlive = keepAlive;
    boolean SPECIAL_DURABLE = Boolean.getBoolean("gemfire.SPECIAL_DURABLE");
        if (cnt > 0) {
        //special case to allow closing durable client pool under the keep alive flag
            //closing regions prior to closing pool can cause them to unregister interest
            if (SPECIAL_DURABLE) {
                synchronized (simpleLock) {
                    try {
                        if (!CacheFactory.getAnyInstance().isClosed() && this.getPoolOrCacheCancelInProgress() == null) {
                            Set<Region<?, ?>> regions = CacheFactory.getInstance(dsys).rootRegions();
                            for (Region<?, ?> roots : regions) {
                                Set<Region<?, ?>> subregions = roots.subregions(true);
                                for (Region<?, ?> subroots : subregions) {
                                    if (!subroots.isDestroyed() &&  subroots.getAttributes().getPoolName() != null
                                            && subroots.getAttributes().getPoolName().equals(this.name) ) {
                                        if (logger.isDebugEnabled()) {
                                            logger.debug("PoolImpl.destroy[ Region connected count:{} Region subroot closing:{} Pool Name:{} ]", cnt, subroots.getName(), this.name);
                                        }
                                        subroots.close();
                                    }
                                }

                                if (!roots.isDestroyed() && roots.getAttributes().getPoolName() != null
                                        && roots.getAttributes().getPoolName().equals(this.name)) {
                                    if (logger.isDebugEnabled()) {
                                        logger.debug("PoolImpl.destroy[ Region connected count:{} Region root closing:{} Pool Name:{} ]", cnt, roots.getName(), this.name);
                                    }
                                    roots.close();
                                }
                            }
                        }
                    } catch (CacheClosedException ccex) {
                      if (logger.isDebugEnabled()) {
                        logger.debug(ccex.getMessage(), ccex);
                      }
                    } catch (Exception ex) {
                      if (logger.isDebugEnabled()) {
                        logger.debug(ex.getMessage(), ex);
                      }
                    }
                }
            } //end special case
        
            cnt = getAttachCount();
    if (cnt > 0) {
      throw new IllegalStateException( LocalizedStrings.PoolImpl_POOL_COULD_NOT_BE_DESTROYED_BECAUSE_IT_IS_STILL_IN_USE_BY_0_REGIONS.toLocalizedString(Integer.valueOf(cnt)));
    }
        }
    if (this.pm.unregister(this)) {
      basicDestroy(keepAlive);
    }
  }

  /**
   * Destroys this pool but does not unregister it.
   * This is used by the PoolManagerImpl when it wants to close all its pools.
   */
  public synchronized void basicDestroy(boolean keepAlive) {
    if (!isDestroyed()) {
      this.destroyed = true;
      // LOG: changed from config to info
      logger.info(LocalizedMessage.create(LocalizedStrings.PoolImpl_DESTROYING_CONNECTION_POOL_0, name));

      try {
        if (backgroundProcessor != null) {
          backgroundProcessor.shutdown();
          if(!backgroundProcessor.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS)) {
            logger.warn(LocalizedMessage.create(LocalizedStrings.PoolImpl_TIMEOUT_WAITING_FOR_BACKGROUND_TASKS_TO_COMPLETE));
          }
        }
      } catch(RuntimeException e) {
        logger.error(LocalizedMessage.create(LocalizedStrings.PoolImpl_ERROR_ENCOUNTERED_WHILE_STOPPING_BACKGROUNDPROCESSOR), e);
      } catch(InterruptedException e) {
        logger.error(LocalizedMessage.create(LocalizedStrings.PoolImpl_INTERRUPTED_WHILE_STOPPING_BACKGROUNDPROCESSOR), e);
      }

      try {
        if (this.source != null) {
          this.source.stop();
        }
      } catch(RuntimeException e) {
        logger.error(LocalizedMessage.create(LocalizedStrings.PoolImpl_ERROR_ENCOUNTERED_WHILE_STOPPING_CONNECTION_SOURCE), e);
      } 

      try {
        if(this.manager != null) {
          manager.close(keepAlive);
        }
      } catch(RuntimeException e) {
        logger.error(LocalizedMessage.create(LocalizedStrings.PoolImpl_ERROR_ENCOUNTERED_WHILE_STOPPING_CONNECTION_MANAGER), e);
      }
      
      try {
        if(this.queueManager != null) {
          queueManager.close(keepAlive);
        }
      } catch(RuntimeException e) {
        logger.error(LocalizedMessage.create(LocalizedStrings.PoolImpl_ERROR_ENCOUNTERED_WHILE_STOPPING_SUBSCRIPTION_MANAGER), e);
      }
      
      try {
        endpointManager.close();
      } catch(RuntimeException e) {
        logger.error(LocalizedMessage.create(LocalizedStrings.PoolImpl_ERROR_ENCOUNTERED_WHILE_STOPPING_ENDPOINT_MANAGER), e);
      }

      try {
        if(this.stats!=null) {
          this.stats.close();
        }
      } catch(RuntimeException e) {
        logger.error(LocalizedMessage.create(LocalizedStrings.PoolImpl_ERROR_WHILE_CLOSING_STATISTICS), e);
      }
    }
  }
  
  public boolean isDestroyed() {
    return destroyed;
  }
  
  
  private ConnectionSource getSourceImpl(LocatorDiscoveryCallback locatorDiscoveryCallback) {
    List<InetSocketAddress> locators = getLocators();
    if (locators.isEmpty()) {
      return new ExplicitConnectionSourceImpl(getServers());
    }
    else {
      AutoConnectionSourceImpl source = new AutoConnectionSourceImpl(locators,
          getServerGroup(), HANDSHAKE_TIMEOUT);
      if(locatorDiscoveryCallback != null) {
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
      throw new RuntimeException( 
          LocalizedStrings.PoolImpl__0_IS_NOT_THE_SAME_AS_1_BECAUSE_IT_SHOULD_HAVE_BEEN_A_POOLIMPL
          .toLocalizedString(new Object[] {this, obj}));
    }
    PoolImpl other = (PoolImpl)obj;
    if (!getName().equals(other.getName())) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_ARE_DIFFERENT.toLocalizedString("names"));
    }
    if (getFreeConnectionTimeout() != other.getFreeConnectionTimeout()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("connectionTimeout"));
    }
    if (getLoadConditioningInterval() != other.getLoadConditioningInterval()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("connectionLifetime"));
    }
    if (getSocketBufferSize() != other.getSocketBufferSize()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("socketBufferSize"));
    }
    if (getThreadLocalConnections() != other.getThreadLocalConnections()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("threadLocalConnections"));
    }
    if (getReadTimeout() != other.getReadTimeout()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("readTimeout"));
    }
    if (getMinConnections() != other.getMinConnections()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("MinConnections"));
    }
    if (getMaxConnections() != other.getMaxConnections()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("MaxConnections"));
    }
    if (getRetryAttempts() != other.getRetryAttempts()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("RetryAttempts"));
    }
    if (getIdleTimeout() != other.getIdleTimeout()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("IdleTimeout"));
    }
    if (getPingInterval() != other.getPingInterval()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("PingInterval"));
    }
    if (getStatisticInterval() != other.getStatisticInterval()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("StatisticInterval"));
    }
    if (getSubscriptionAckInterval() != other.getSubscriptionAckInterval()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("subscriptionAckInterval"));
    }
    if (getSubscriptionEnabled() != other.getSubscriptionEnabled()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("subscriptionEnabled"));
    }
    if (getSubscriptionMessageTrackingTimeout() != other.getSubscriptionMessageTrackingTimeout()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("subscriptionMessageTrackingTimeout"));
    }
    if (getSubscriptionRedundancy() != other.getSubscriptionRedundancy()) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("subscriptionRedundancyLevel"));
    }
    if (!getServerGroup().equals(other.getServerGroup())) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_IS_DIFFERENT.toLocalizedString("serverGroup"));
    }
    if (!getLocators().equals(other.getLocators())) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_ARE_DIFFERENT.toLocalizedString("locators"));
    }
    if (!getServers().equals(other.getServers())) {
      throw new RuntimeException(
          LocalizedStrings.PoolImpl_0_ARE_DIFFERENT.toLocalizedString("servers"));
    }
    // ignore startDisabled
  }
  
  public PoolStats getStats() {
    return this.stats;
  }


  /**
   * Execute the given op on the servers that this pool connects to.
   * This method is responsible for retrying the op if an attempt fails.
   * It will only execute it once and on one server.
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   * @since 5.7
   */
  public Object execute(Op op) {
    //if(multiuser)
    //get a server from threadlocal cache else throw cacheWriterException 
    //executeOn(ServerLocation server, Op op, boolean accessed,boolean onlyUseExistingCnx)

    // Retries are ignored here. FIX IT - FIXED.
    // But this may lead to a user getting authenticated on all servers, even if
    // a single server could have serviced all its requests.
    authenticateIfRequired(op);
    return executor.execute(op);
  }

  /**
   * Execute the given op on the servers that this pool connects to.
   * This method is responsible for retrying the op if an attempt fails.
   * It will only execute it once and on one server.
   * @param op the operation to execute
   * @param retries how many times to retry the operation
   * @return the result of execution if any; null if not
   * @since 5.7
   */
  public Object execute(Op op, int retries) {
    authenticateIfRequired(op);
    return executor.execute(op, retries);
  }

  /**
   * Execute the given op on the given server.
   * @param server the server to do the execution on
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  public Object executeOn(ServerLocation server, Op op) {
    authenticateIfRequired(server, op);
    return executor.executeOn(server, op);
  }
  /**
   * Execute the given op on the given server.
   * @param server the server to do the execution on
   * @param op the operation to execute
   * @param accessed true if the connection is accessed by this execute
   * @return the result of execution if any; null if not
   */
  public Object executeOn(ServerLocation server, Op op, boolean accessed,boolean onlyUseExistingCnx) {
    authenticateIfRequired(server, op);
    return executor.executeOn(server, op, accessed,onlyUseExistingCnx);
  }
  
  /**
   * Execute the given op on the given connection.
   * @param con the connection to do the execution on
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  public Object executeOn(Connection con, Op op) {
    authenticateIfRequired(con.getServer(), op);
    return executor.executeOn(con, op);
  }

  public Object executeOn(Connection con, Op op, boolean timeoutFatal) {
    return executor.executeOn(con, op, timeoutFatal);
  }

  /**
   * Execute the given op on all the servers that have server-to-client
   * queues for this pool
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   * @since 5.7
   */
  public Object executeOnQueuesAndReturnPrimaryResult(Op op) {
    authenticateOnAllServers(op);
    return executor.executeOnQueuesAndReturnPrimaryResult(op);
  }

  public void executeOnAllQueueServers(Op op)
    throws NoSubscriptionServersAvailableException, SubscriptionNotEnabledException {
    authenticateOnAllServers(op);
    executor.executeOnAllQueueServers(op);
  }

  /**
   * Execute the given op on the current primary server.
   * @param op the operation to execute
   * @return the result of execution if any; null if not
   */
  public Object executeOnPrimary(Op op) {
    return executor.executeOnPrimary(op);
  }
  
  public Map<ServerLocation, Endpoint> getEndpointMap() {
    return endpointManager.getEndpointMap();
  }
  
  public ScheduledExecutorService getBackgroundProcessor() {
    return backgroundProcessor;
  }
  
  public RegisterInterestTracker getRITracker() {
    return this.riTracker;
  }

  /**
   * Test hook that returns the number of servers we currently have connections to.
   */
  public int getConnectedServerCount() {
    return this.endpointManager.getConnectedServerCount();
  }
  /**
   * Test hook.
   * Verify if this EventId is already present in the map or not. If it is
   * already present then return true.
   *
   * @param eventId the EventId of the incoming event
   * @return true if it is already present
   * @since 5.1
   */
  public boolean verifyIfDuplicate(EventID eventId) {
    return ((QueueStateImpl)this.queueManager.getState()).verifyIfDuplicate(eventId);
  }

  public boolean verifyIfDuplicate(EventID eventId, boolean addToMap) {
    return ((QueueStateImpl)this.queueManager.getState()).verifyIfDuplicate(eventId);
  }

  /**
   * Borrows a connection from the pool.. Used by gateway and tests.
   * Any connection that is acquired using this method must be returned using
   * returnConnection, even if it is destroyed.
   * 
   * TODO - The use of the this method should be removed
   * from the gateway code. This method is fine for tests,
   * but these connections should really be managed inside
   * the pool code. If the gateway needs to persistent connection
   * to a single server, which should create have the OpExecutor
   * that holds a reference to the connection (similar to the way
   * we do with thread local connections).
   * TODO use {@link ExecutablePool#setupServerAffinity(boolean)} for
   * gateway code
   */
  public Connection acquireConnection() {
    return manager.borrowConnection(45000L);
  }
  
  /**
   * Hook to return connections that were acquired using 
   * acquireConnection.
   * @param conn
   */
  public void returnConnection(Connection conn) {
    manager.returnConnection(conn);
  }
  
  /**
   * Test hook that acquires and returns a connection from the pool with a given ServerLocation.
   */
  public Connection acquireConnection(ServerLocation loc) {
    return manager.borrowConnection(loc,15000L,false);
  }

  /**
   * Test hook that returns an unnmodifiable list of the current blacklisted servers
   */
  public Set getBlacklistedServers() {
    return connectionFactory.getBlackList().getBadServers();
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
    if (this.queueManager == null) return Collections.EMPTY_MAP;
    if (this.queueManager.getState() == null) return Collections.EMPTY_MAP;
    return this.queueManager.getState().getThreadIdToSequenceIdMap();
  }
  
  /**
   * Test hook that returns true if we have a primary and its updater thread
   * is alive.
   */
  public boolean isPrimaryUpdaterAlive() {
    return ((QueueManagerImpl)this.queueManager).isPrimaryUpdaterAlive();
  }
  /**
   * Test hook used to simulate a kill of the primaryEndpoint
   */
  public void killPrimaryEndpoint() //throws ServerException
  {
    boolean ok = false;
    if (this.queueManager != null) {
      QueueManager.QueueConnections cons = this.queueManager.getAllConnections();
      Connection con = cons.getPrimary();
      if (con != null) {
        final String msg = "killing primary endpoint";
        logger.info("<ExpectedException action=add>{}</ExpectedException>", msg);
        Exception e = new Exception(msg);
        try {
          processException(e, con);
        } catch (ServerConnectivityException expected) {
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

  // Pool that are declared in a cache.xml will set this property to true.
  private boolean declaredInXML;

  public void setDeclaredInXML(boolean v) {
    this.declaredInXML = v;
  }
  public boolean getDeclaredInXML() {
    return this.declaredInXML;
  }

  // used by unit tests to confirm if readyForEvents has been called on a pool
  private boolean readyForEventsCalled;

  public boolean getReadyForEventsCalled() {
    return this.readyForEventsCalled;
  }
  
  public void readyForEvents(InternalDistributedSystem system) {
    if(!isDurableClient() || queueManager == null) {
      return;
    }
    this.readyForEventsCalled = true;
    queueManager.readyForEvents(system);
    
  }
  
  public boolean isDurableClient() {
    boolean isDurable = false;
    InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
    DistributionConfig config = system.getConfig();
    String durableClientId = config.getDurableClientId();
    isDurable = durableClientId != null && durableClientId.length() > 0;
    return isDurable;
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
  /**
   * Test hook that returns an int which the port of the primary server.
   * -1 is returned if we have no primary.
   */
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
    if (this.queueManager != null) {
      QueueManager.QueueConnections cons = this.queueManager.getAllConnections();
      Connection con = cons.getPrimary();
      result = con.getServer();
    }
    return result;
  }
  
  /**
   * Test hook to get a connection to the primary server.
   */
  public Connection getPrimaryConnection() {
    if (this.queueManager != null) {
      QueueManager.QueueConnections cons = this.queueManager.getAllConnections();
      return cons.getPrimary();
    }
    return null;
  }
  
  /**
   * Test hook that returns a list of strings. Each string consists of the host name and port of a redundant server.
   * An empty list is returned if we have no redundant servers.
   */
  public List<String> getRedundantNames() {
    List result = Collections.EMPTY_LIST;
    if (this.queueManager != null) {
      QueueManager.QueueConnections cons = this.queueManager.getAllConnections();
      List<Connection> backupCons = cons.getBackups();
      if (backupCons.size() > 0) {
        result = new ArrayList(backupCons.size());
        Iterator<Connection> it = backupCons.iterator();
        while (it.hasNext()) {
          Connection con = it.next();
          ServerLocation sl = con.getServer();
          result.add(sl.getHostName() + sl.getPort());
        }
      }
    }
    return result;
  }
  /**
   * Test hook that returns a list of ServerLocation instances.
   * Each ServerLocation describes a redundant server.
   * An empty list is returned if we have no redundant servers.
   */
  public List<ServerLocation> getRedundants() {
    List result = Collections.EMPTY_LIST;
    if (this.queueManager != null) {
      QueueManager.QueueConnections cons = this.queueManager.getAllConnections();
      List<Connection> backupCons = cons.getBackups();
      if (backupCons.size() > 0) {
        result = new ArrayList(backupCons.size());
        Iterator<Connection> it = backupCons.iterator();
        while (it.hasNext()) {
          Connection con = it.next();
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
   * @since 5.7
   */
  private final AtomicInteger attachCount = new AtomicInteger();
  public static volatile boolean IS_INSTANTIATOR_CALLBACK = false ;

  /**
   * Returns number of services currently using/attached to this pool.
   * <p>Made public so it can be used by tests
   * @since 5.7
   */
  public int getAttachCount() {
    return this.attachCount.get();
  }
  /**
   * This needs to be called when a service (like a Region or CQService)
   * starts using a pool.
   * @since 5.7
   */
  public void attach() {
    this.attachCount.getAndIncrement();
  }
  /**
   * This needs to be called when a service (like a Region or CQService)
   * stops using a pool.
   * @since 5.7
   */
  public void detach() {
    this.attachCount.getAndDecrement();
  }

  /**
   * Get the connection held by this thread
   * if we're using thread local connections
   * 
   * This is a a hook for hydra code to pass
   * thread local connections between threads.
   * @return the connection from the thread local,
   * or null if there is no thread local connection.
   */
  public Connection getThreadLocalConnection() {
    return executor.getThreadLocalConnection();
  }

  /**
   * Returns a list of ServerLocation instances;
   * one for each server we are currently connected to.
   */
  public List<ServerLocation> getCurrentServers() {
    ArrayList result = new ArrayList();
    Map endpointMap = endpointManager.getEndpointMap();
    result.addAll(endpointMap.keySet());
    return result;
  }
  /**
   * Test hook that returns a list of server names (host+port);
   * one for each server we are currently connected to.
   */
  public List<String> getCurrentServerNames() {
    List<ServerLocation> servers = getCurrentServers();
    ArrayList<String> result = new ArrayList(servers.size());
    Iterator it = servers.iterator();
    while (it.hasNext()) {
      ServerLocation sl = (ServerLocation)it.next();
      String name = sl.getHostName() + sl.getPort();
      result.add(name);
    }
    return result;
  }
  
  public EndpointManager getEndpointManager() {
    return endpointManager;
  }

  /**
   * Fetch the connection source for this pool
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
      java.lang.Thread.sleep(this.pingInterval * 2);
    }
    catch (java.lang.InterruptedException ex) {
      // do nothing.
    }
    
    Map endpoints = endpointManager.getEndpointMap();
    for(Iterator itr = endpoints.values().iterator(); itr.hasNext();) {
      Endpoint endpoint = (Endpoint) itr.next();
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
      java.lang.Thread.sleep(this.pingInterval * 2);
    }
    catch (java.lang.InterruptedException ex) {
      // do nothing.
    }
  }
  /**
   * test hook
   */
  public int getInvalidateCount() {
    return ((QueueStateImpl)this.queueManager.getState()).getInvalidateCount();
  }
  /**
   * Set the connection held by this thread
   * if we're using thread local connections
   * 
   * This is a a hook for hydra code to pass
   * thread local connections between threads.
   */
  public void setThreadLocalConnection(Connection conn) {
    executor.setThreadLocalConnection(conn);
  }
  
  public ServerLocation getServerAffinityLocation() {
    return executor.getServerAffinityLocation();
  }
  
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
  public static volatile boolean AFTER_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;

  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  public static volatile boolean BEFORE_REGISTER_CALLBACK_FLAG = false;

  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  public static volatile boolean BEFORE_RECOVER_INTEREST_CALLBACK_FLAG = false;

  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  public static volatile boolean AFTER_REGISTER_CALLBACK_FLAG = false;

  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  public static volatile boolean BEFORE_PRIMARY_IDENTIFICATION_FROM_BACKUP_CALLBACK_FLAG = false;

  /**
   * A debug flag used for testing used in ClientServerObserver
   */
  public static volatile boolean BEFORE_SENDING_CLIENT_ACK_CALLBACK_FLAG = false;
  /**
   * A debug flag used for testing used in ClientServerObserver
   */  
  public static volatile boolean AFTER_QUEUE_DESTROY_MESSAGE_FLAG = false;
  
  /**
   * Test hook flag to notify observer(s) that a primary is recovered
   * either from a backup or from a new connection.
   */
  public static volatile boolean AFTER_PRIMARY_RECOVERED_CALLBACK_FLAG = false;
  
  public static abstract class PoolTask implements Runnable {
    
    public final void run() {
      try {
        run2();
      } catch(VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } 
      catch (CancelException e) {
//        throw e;
        if (logger.isDebugEnabled()) {
          logger.debug("Pool task <{}> cancelled", this, logger.isTraceEnabled() ? e : null);
        }
      } catch(Throwable t) {
        logger.error(LocalizedMessage.create(LocalizedStrings.PoolImpl_UNEXPECTED_ERROR_IN_POOL_TASK_0, this), t);
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
   * A cancel criterion that checks both the pool and the cache
   * for canceled status.
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
   * A cancel criterion that checks only if this pool has been
   * closed. This is necessary because there are some things that
   * we want to allow even after the cache has started closing.
   */
  protected class Stopper extends CancelCriterion {

    @Override
    public String cancelInProgress() {
      if(destroyed) {
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

  public static void loadEmergencyClasses() {
    QueueManagerImpl.loadEmergencyClasses();
    ConnectionManagerImpl.loadEmergencyClasses();
    EndpointManagerImpl.loadEmergencyClasses();
  }
  
  /**
   * Returns the QueryService, that can be used to execute Query functions on 
   * the servers associated with this pool.
   * @return the QueryService 
   */
  public QueryService getQueryService() {
    Cache cache = CacheFactory.getInstance(InternalDistributedSystem.getAnyInstance());
    DefaultQueryService queryService = new DefaultQueryService((InternalCache) cache);
    queryService.setPool(this);
    return queryService;
  }

  public RegionService createAuthenticatedCacheView(Properties properties) {
    if (!this.multiuserSecureModeEnabled) {
      throw new UnsupportedOperationException(
          "Operation not supported when multiuser-authentication is false.");
    }
    if (properties == null || properties.isEmpty()) {
      throw new IllegalArgumentException("Security properties cannot be empty.");
    }
    Cache cache = CacheFactory.getInstance(InternalDistributedSystem
        .getAnyInstance());

    Properties props = new Properties();
    for (Entry<Object, Object> entry : properties.entrySet()) {
      props.setProperty((String)entry.getKey(), (String)entry.getValue());
    }
    ProxyCache proxy = new ProxyCache(props, (GemFireCacheImpl)cache, this);
    synchronized (this.proxyCacheList) {
      this.proxyCacheList.add(proxy);
    }
    return proxy;
  }

  private volatile CancelCriterion cacheCriterion = null;
  
  private RuntimeException generatePoolOrCacheCancelledException(Throwable e) {
    RuntimeException re = getCancelCriterion().generateCancelledException(e);
    if(re != null) {
      return re;
    }
    Cache cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      if (cacheCriterion != null) {
        return cacheCriterion.generateCancelledException(e);
      }
    } else {
      if (cacheCriterion == null) {
        cacheCriterion = cache.getCancelCriterion();
      } else if (cacheCriterion != cache.getCancelCriterion()) {
        /*
         * If the cache instance has somehow changed, we need to get a reference
         * to the new criterion. This is pretty unlikely because the cache
         * closes all the pools when it shuts down, but I wanted to be safe.
         */
        cacheCriterion = cache.getCancelCriterion();
      }
      return cacheCriterion.generateCancelledException(e);
    }
    return null;
  }

  public String getPoolOrCacheCancelInProgress() {
    String reason = null;
    try {
      reason = getCancelCriterion().cancelInProgress();
      if(reason!=null) {
        return reason;
      }
      Cache cache = GemFireCacheImpl.getInstance();
      if(cache==null) {
         if(cacheCriterion!=null) {
            return cacheCriterion.cancelInProgress();
	 }
         return null;
      } else {
        if(cacheCriterion==null) {
	  cacheCriterion = cache.getCancelCriterion();
	} else if(cacheCriterion!=cache.getCancelCriterion()) {
	  /* 
	  If the cache instance has somehow changed, we need to 
	  get a reference to the new criterion. This is pretty unlikely
	  because the cache closes all the pools when it shuts down,
	  but I wanted to be safe.
	  */
	  cacheCriterion = cache.getCancelCriterion();
	}
        return cacheCriterion.cancelInProgress();  
      }
    } catch(CancelException cce) {
      if(cce.getMessage()!=null) {
        return cce.getMessage();
      } else {
        return "cache is closed";
      }
    } 
  }

  public ArrayList<ProxyCache> getProxyCacheList() {
    return this.proxyCacheList;
  }

  private void authenticateIfRequired(Op op) {
    authenticateIfRequired(null, op);
  }

  /**
   * Assert thread-local var is not null, if it has
   * multiuser-authentication set to true.
   * 
   * If serverLocation is non-null, check if the the user is authenticated on
   * that server. If not, authenticate it and return.
   * 
   * @param serverLocation
   * @param op
   */
  private void authenticateIfRequired(ServerLocation serverLocation, Op op) {
    if (this.multiuserSecureModeEnabled && op instanceof AbstractOp
        && ((AbstractOp)op).needsUserId()) {
      UserAttributes userAttributes = UserAttributes.userAttributes.get();
      if (userAttributes == null) {
        throw new UnsupportedOperationException(LocalizedStrings.MultiUserSecurityEnabled_USE_POOL_API.toLocalizedString());
      }
      if (serverLocation != null) {
        if (!userAttributes.getServerToId().containsKey(serverLocation)) {
          Long userId = (Long)AuthenticateUserOp.executeOn(serverLocation,
              this, userAttributes.getCredentials());
          if (userId != null) {
            userAttributes.setServerToId(serverLocation, userId);
          }
        }
      }
    }
  }

  private void authenticateOnAllServers(Op op) {
    if (this.multiuserSecureModeEnabled && ((AbstractOp)op).needsUserId()) {
      UserAttributes userAttributes = UserAttributes.userAttributes.get();
      if (userAttributes != null) {
        ConcurrentHashMap<ServerLocation, Long> map = userAttributes
            .getServerToId();

        if (this.queueManager == null) {
          throw new SubscriptionNotEnabledException();
        }
        Connection primary = this.queueManager.getAllConnectionsNoWait()
            .getPrimary();
        if (primary != null && !map.containsKey(primary.getServer())) {
          Long userId = (Long)AuthenticateUserOp.executeOn(primary.getServer(),
              this, userAttributes.getCredentials());
          if (userId != null) {
            map.put(primary.getServer(), userId);
          }
        }

        List<Connection> backups = this.queueManager.getAllConnectionsNoWait().getBackups();
        for (int i = 0; i < backups.size(); i++) {
          Connection conn = backups.get(i);
          if (!map.containsKey(conn.getServer())) {
            Long userId = (Long)AuthenticateUserOp.executeOn(conn.getServer(),
                this, userAttributes.getCredentials());
            if (userId != null) {
              map.put(conn.getServer(), userId);
            }
          }
        }
      } else {
        throw new UnsupportedOperationException(LocalizedStrings.MultiUserSecurityEnabled_USE_POOL_API.toLocalizedString());
      }
    }
  }

  public void setPendingEventCount(int count) {
    this.primaryQueueSize.set(count);
  }

  public int getPendingEventCount() {
    if(!isDurableClient() || this.queueManager == null) {
      throw new IllegalStateException(LocalizedStrings.PoolManagerImpl_ONLY_DURABLE_CLIENTS_SHOULD_CALL_GETPENDINGEVENTCOUNT.toLocalizedString());
    }
    if (this.readyForEventsCalled) {
      throw new IllegalStateException(LocalizedStrings.PoolManagerImpl_GETPENDINGEVENTCOUNT_SHOULD_BE_CALLED_BEFORE_INVOKING_READYFOREVENTS.toLocalizedString());
    }
    return this.primaryQueueSize.get();
  }
}
