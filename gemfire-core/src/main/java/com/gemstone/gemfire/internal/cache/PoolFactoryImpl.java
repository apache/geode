/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.RegionService;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.internal.BridgePoolImpl;
import com.gemstone.gemfire.cache.client.internal.BridgePoolImpl.LBPolicy;
import com.gemstone.gemfire.cache.client.internal.LocatorDiscoveryCallback;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;

/**
 * Implementation of PoolFactory.
 * @author darrel
 * @since 5.7
 */
public class PoolFactoryImpl implements PoolFactory {
  private static final Logger logger = LogService.getLogger();
  
  /**
   * Used internally to pass the attributes from this factory to
   * the real pool it is creating.
   */
  private PoolAttributes attributes = new PoolAttributes();
  
  /**
   * The cache that created this factory
   */
  private final PoolManagerImpl pm; 

  public PoolFactoryImpl(PoolManagerImpl pm) {
    this.pm = pm;
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
    if(idleTimout < -1) {
      throw new IllegalArgumentException("idleTimeout must be greater than or equal to -1");
    }
    this.attributes.idleTimeout = idleTimout;
    return this;
  }

  public PoolFactory setMaxConnections(int maxConnections) {
    if(maxConnections < this.attributes.minConnections && maxConnections != -1) {
      throw new IllegalArgumentException(
          "maxConnections must be greater than or equal to minConnections ("
              + attributes.minConnections + ")");
    }
    if(maxConnections <= 0 && maxConnections != -1) {
      throw new IllegalArgumentException(
          "maxConnections must be greater than 0, or set to -1 (no max)");
    }
    this.attributes.maxConnections = maxConnections;
    return this;
  }

  public PoolFactory setMinConnections(int minConnections) {
    if(minConnections > attributes.maxConnections && attributes.maxConnections != -1) {
      throw new IllegalArgumentException(
          "must be less than or equal to maxConnections (" + attributes.maxConnections + ")");
    }
    if(minConnections < 0) {
      throw new IllegalArgumentException(
          "must be greater than or equal to 0");
    }
    this.attributes.minConnections=minConnections;
    return this;
  }

  public PoolFactory setPingInterval(long pingInterval) {
    if(pingInterval <= 0) {
      throw new IllegalArgumentException("pingInterval must be greater than zero");
    }
    this.attributes.pingInterval=pingInterval;
    return this;
  }

  public PoolFactory setStatisticInterval(int statisticInterval) {
    if(statisticInterval < -1) {
      throw new IllegalArgumentException("statisticInterval must be greater than or equal to -1");
    }
    this.attributes.statisticInterval = statisticInterval;
    return this;
  }

  public PoolFactory setRetryAttempts(int retryAttempts) {
    if(retryAttempts < -1) {
      throw new IllegalArgumentException("retryAttempts must be greater than or equal to -1");
    }
    this.attributes.retryAttempts=retryAttempts;
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
      throw new IllegalArgumentException("queueRedundancyLevel must be greater than or equal to -1");
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

  private PoolFactory add(String host, int port, List l) {
    if (port == 0) {
      throw new IllegalArgumentException("port must be greater than 0 but was " + port);
      // the rest of the port validation is done by InetSocketAddress
    }
    try {
      InetAddress hostAddr = InetAddress.getByName(host);
      InetSocketAddress sockAddr = new InetSocketAddress(hostAddr, port);
      l.add(sockAddr);
    } catch (UnknownHostException cause) {
//      IllegalArgumentException ex = new IllegalArgumentException("Unknown host " + host);
//      ex.initCause(cause);
//      throw ex;
      // Fix for #45348
      logger.warn(LocalizedMessage.create(LocalizedStrings.PoolFactoryImpl_HOSTNAME_UNKNOWN, host));
      InetSocketAddress sockAddr = new InetSocketAddress(host, port);
      l.add(sockAddr);
    }
    return this;
  }

  public PoolFactory setSubscriptionAckInterval(int ackInterval) {
    if(ackInterval <= 0) {
      throw new IllegalArgumentException("ackInterval must be greater than 0");
    }
    this.attributes.queueAckInterval = ackInterval;
    
    return this;
  }

  public PoolFactory addLocator(String host, int port) {
    if (this.attributes.servers.size() > 0) {
      throw new IllegalStateException("A server has already been added. You can only add locators or servers; not both.");
    }
    return add(host, port, this.attributes.locators);
  }
  public PoolFactory addServer(String host, int port) {
    if (this.attributes.locators.size() > 0) {
      throw new IllegalStateException("A locator has already been added. You can only add locators or servers; not both.");
    }
    return add(host, port, this.attributes.servers);
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
    this.attributes.locators.addAll(cp.getLocators());
    this.attributes.servers.addAll(cp.getServers());
  }

  public void init(Properties properties, boolean usedByBridgeWriter,
      boolean usedByGateway, GatewaySender sender) {
    this.attributes.setGatewaySender(sender);
    init(properties, usedByBridgeWriter, usedByGateway);
  }
  /**
   * Used to create a pool given the old Bridge properties
   * @param properties from a BridgeWriter or BridgeLoader
   * @param usedByBridgeWriter true  if props from BridgeWriter;
   *                           false if props from BridgeLoader.
   * *param usedByGateway true if props from GatewayImpl.
   * @since 5.7
   */
  public void init(Properties properties, boolean usedByBridgeWriter,
                   boolean usedByGateway) {
    this.attributes.setBridge(usedByBridgeWriter || !usedByGateway);
    this.attributes.setBridgeWriter(usedByBridgeWriter);
    this.attributes.setGateway(usedByGateway);
    setIdleTimeout(-1); // never time out
    setLoadConditioningInterval(-1); // never time out
    setMaxConnections(-1);
    int endpointCount = 0;
    boolean useLocators = false;
    boolean useEndPoints = false;
    IllegalArgumentException exception = null;
    if (properties.containsKey(DistributionConfig.LOCATORS_NAME)) {
      String locatorObject = properties
          .getProperty(DistributionConfig.LOCATORS_NAME);
      if (locatorObject != null && !locatorObject.equals("")) {
        StringTokenizer locatorsOnThisVM = new StringTokenizer(locatorObject, ",");
        while (locatorsOnThisVM.hasMoreTokens()) {
          String localLocator = locatorsOnThisVM.nextToken();
          DistributionLocatorId locatorId = new DistributionLocatorId(
              localLocator);
          addLocator(locatorId.getHost().getHostName(), locatorId.getPort());
        }
        useLocators = true;
      }
    }
    if (!useLocators && properties.containsKey("endpoints")) {
      useEndPoints = true;
      String pv = properties.getProperty("endpoints");
      StringTokenizer tokenizer = new StringTokenizer(pv, ",");
      while (tokenizer.hasMoreTokens()) {
        String serverdetail = tokenizer.nextToken();
        int cIndex = serverdetail.indexOf("=");
        // note we throw the name away
//         String name = serverdetail.substring(0, cIndex);
//         if (name != null) {
//           name = name.trim();
//         }
        String remainder = serverdetail.substring(cIndex + 1);
        cIndex = remainder.lastIndexOf(":");
        String host = remainder.substring(0, cIndex);
        if (host != null) {
          host = host.trim();
        }
        String port = remainder.substring(cIndex + 1);
        if (port != null) {
          port = port.trim();
        }
        try {
          addServer(host, Integer.parseInt(port));
          endpointCount++;
        } catch (IllegalArgumentException e) {
          if (!(e.getCause() instanceof UnknownHostException)) {
            throw e;
          } else {
            exception = e;
          }
        }
      }
      if ((endpointCount == 0) && (exception != null)) {
        IllegalArgumentException ex = new IllegalArgumentException("Couldn't find any Endpoint. " + exception.getMessage());
        ex.initCause(exception.getCause());
        throw ex;
      }
    }
    if(!useLocators && !useEndPoints) {
      throw new IllegalArgumentException(
          "Property 'locators ' or 'endpoints' must be specified");
    }
    // @todo grid: handshakeTimeout ignored
    {
      // @todo grid: roundRobin and appAssisted ignored
      LBPolicy policy = new LBPolicy(properties.getProperty("LBPolicy",
          LBPolicy.STICKY_PROPERTY_NAME));
      setThreadLocalConnections(policy.isSticky());
    }
    
    if (properties.containsKey("retryAttempts")) {
      String strRetryAttempts = properties.getProperty("retryAttempts");
      setRetryAttempts(Integer.parseInt(strRetryAttempts));
    }
    if (properties.containsKey("retryInterval")) {
      String strRetryAttempts = properties.getProperty("retryInterval");
      setPingInterval(Integer.parseInt(strRetryAttempts));
    }
    if (properties.containsKey("establishCallbackConnection")) {
      String str = properties.getProperty("establishCallbackConnection");
      setSubscriptionEnabled(Boolean.valueOf(str).booleanValue());
    }
    if (properties.containsKey("enablePRSingleHop")) {
      String str = properties.getProperty("enablePRSingleHop");
      setPRSingleHopEnabled(Boolean.valueOf(str).booleanValue());
    }
    if (properties.containsKey("connectionsPerServer")) {
      String str = properties.getProperty("connectionsPerServer");
      setMinConnections(Integer.parseInt(str)*endpointCount);
    } else {
      setMinConnections(1*endpointCount);
    }
    if (properties.containsKey("redundancyLevel")) {
      String str = properties.getProperty("redundancyLevel");
      setSubscriptionRedundancy(Integer.parseInt(str));
    }
    if (properties.containsKey("readTimeout")) {
      String strReadTimeout = properties.getProperty("readTimeout");
      setReadTimeout(Integer.parseInt(strReadTimeout));
    }
    if (properties.containsKey("socketBufferSize")) {
      String strSocketBufferSize = properties.getProperty("socketBufferSize");
      setSocketBufferSize(Integer.parseInt(strSocketBufferSize));
    }
    if (properties.containsKey("messageTrackingTimeout")) {
      String pv = properties.getProperty("messageTrackingTimeout");
      setSubscriptionMessageTrackingTimeout(Integer.parseInt(pv));
    }
    if(properties.containsKey("clientAckInterval") ) {
      String pv = properties.getProperty("clientAckInterval");
      setSubscriptionAckInterval(Integer.parseInt(pv));
    }
    if(usedByGateway && exception!= null) {
      throw exception;
    }
  }
  
  /**
   * Create a new Pool for connecting a client to a set of GemFire Cache Servers.
   * using this factory's settings for attributes.
   * 
   * @param name the name of the connection pool, used when connecting regions to it
   * @throws IllegalStateException if the connection pool name already exists
   * @throws IllegalStateException if this factory does not have any locators or servers
   * @return the newly created connection pool.
   * @since 5.7
   */
  public Pool create(String name) throws CacheException {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if(cache != null) {
      TypeRegistry registry = cache.getPdxRegistry();
      if(registry != null && !attributes.isGateway()) {
        registry.creatingPool();
      }
    }
    if (this.attributes.isBridge()) {
      return new BridgePoolImpl(this.pm, name, this.attributes);
    } else {
      return PoolImpl.create(this.pm, name, this.attributes);
    }
  }

  /**
   * Needed by test framework.
   */
  public PoolAttributes getPoolAttributes() {
    return this.attributes;
  }

  /**
   * Not a true pool just the attributes.
   * Serialization is used by unit tests
   */
  public static class PoolAttributes implements Pool, DataSerializable {

    private static final long serialVersionUID = 1L; // for findbugs

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
    public String serverGroup = DEFAULT_SERVER_GROUP;
    public boolean multiuserSecureModeEnabled = DEFAULT_MULTIUSER_AUTHENTICATION;
    public ArrayList/*<InetSocketAddress>*/ locators = new ArrayList();
    public ArrayList/*<InetSocketAddress>*/ servers = new ArrayList();
    public transient boolean startDisabled = false; // only used by junit tests
    public transient LocatorDiscoveryCallback locatorCallback = null; //only used by tests
    public GatewaySender gatewaySender = null;
    /**
     * True if this factory needs to produce a pool for use by BridgeWriter
     * or BridgeLoader.
     */
    public boolean bridge = false;
    /**
     * True if bridge is true and the pool is used by a BridgeWriter.
     * False if bridge is true and the pool is used by a BridgeLoader.
     * Ignore this attribute if bridge is false.
     */
    public boolean bridgeWriter = false;
    /**
     * True if bridge is true and the pool is used by a Gateway.
     * Ignore this attribute if bridge is false.
     */
    public boolean gateway = false;

    public int getFreeConnectionTimeout() {
      return this.connectionTimeout;
    }
    public int getLoadConditioningInterval() {
      return this.connectionLifetime;
    }
    public int getSocketBufferSize() {
      return this.socketBufferSize;
    }
    public int getMinConnections() {
      return minConnections;
    }
    public int getMaxConnections() {
      return maxConnections;
    }
    public long getIdleTimeout() {
      return idleTimeout;
    }
    public int getRetryAttempts() {
      return retryAttempts;
    }
    public long getPingInterval() {
      return pingInterval;
    }
    public int getStatisticInterval() {
      return statisticInterval;
    }
    public boolean getThreadLocalConnections() {
      return this.threadLocalConnections;
    }
    public int getReadTimeout() {
      return this.readTimeout;
    }
    public boolean getSubscriptionEnabled() {
      return this.queueEnabled;
    }
    public boolean getPRSingleHopEnabled() {
      return this.prSingleHopEnabled;
    }
    public int getSubscriptionRedundancy() {
      return this.queueRedundancyLevel;
    }
    public int getSubscriptionMessageTrackingTimeout() {
      return this.queueMessageTrackingTimeout;
    }
    public int getSubscriptionAckInterval() {
      return queueAckInterval;
    }
    public String getServerGroup() {
      return this.serverGroup;
    }
    public boolean isBridge() {
      return this.bridge;
    }
    public void setBridge(boolean v) {
      this.bridge = v;
    }
    public boolean isBridgeWriter() {
      return this.bridgeWriter;
    }
    public void setBridgeWriter(boolean v) {
      this.bridgeWriter = v;
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
    public GatewaySender getGatewaySender(){
      return this.gatewaySender;
    }
    public boolean getMultiuserAuthentication() {
      return this.multiuserSecureModeEnabled;
    }
    public void setMultiuserSecureModeEnabled(boolean v) {
     this.multiuserSecureModeEnabled = v; 
    }    
    public List/*<InetSocketAddress>*/ getLocators() {
      if (this.locators.size() == 0 && this.servers.size() == 0) {
        throw new IllegalStateException("At least one locator or server must be added before a connection pool can be created.");
      }
      // needs to return a copy.
      return Collections.unmodifiableList(new ArrayList(this.locators));
    }
    public List/*<InetSocketAddress>*/ getServers() {
      if (this.locators.size() == 0 && this.servers.size() == 0) {
        throw new IllegalStateException("At least one locator or server must be added before a connection pool can be created.");
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

    public RegionService createAuthenticatedCacheView(Properties properties) {
      throw new UnsupportedOperationException();
    }

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
      DataSerializer.writePrimitiveBoolean(this.bridge, out);
      DataSerializer.writePrimitiveInt(this.statisticInterval, out);
      DataSerializer.writePrimitiveBoolean(this.multiuserSecureModeEnabled,out);
    }
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException
    {
      this.connectionTimeout = DataSerializer.readPrimitiveInt(in);
      this.connectionLifetime = DataSerializer.readPrimitiveInt(in);
      this.socketBufferSize = DataSerializer.readPrimitiveInt(in);
      this.readTimeout = DataSerializer.readPrimitiveInt(in);
      this.minConnections= DataSerializer.readPrimitiveInt(in);
      this.maxConnections= DataSerializer.readPrimitiveInt(in);
      this.retryAttempts= DataSerializer.readPrimitiveInt(in);
      this.idleTimeout= DataSerializer.readPrimitiveLong(in);
      this.pingInterval= DataSerializer.readPrimitiveLong(in);
      this.queueRedundancyLevel = DataSerializer.readPrimitiveInt(in);
      this.queueMessageTrackingTimeout = DataSerializer.readPrimitiveInt(in);
      this.threadLocalConnections = DataSerializer.readPrimitiveBoolean(in);
      this.queueEnabled = DataSerializer.readPrimitiveBoolean(in);
      this.serverGroup = DataSerializer.readString(in);
      this.locators = DataSerializer.readArrayList(in);
      this.servers = DataSerializer.readArrayList(in);
      this.bridge = DataSerializer.readPrimitiveBoolean(in);
      this.statisticInterval= DataSerializer.readPrimitiveInt(in);
      this.multiuserSecureModeEnabled = DataSerializer.readPrimitiveBoolean(in);
    }
  }
}
