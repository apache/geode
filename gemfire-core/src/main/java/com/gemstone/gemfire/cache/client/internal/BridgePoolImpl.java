/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.NoSubscriptionServersAvailableException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.SubscriptionNotEnabledException;
import com.gemstone.gemfire.cache.client.internal.GetOp.GetOpImpl;
import com.gemstone.gemfire.cache.util.BridgeWriterException;
import com.gemstone.gemfire.cache.util.EndpointDoesNotExistException;
import com.gemstone.gemfire.cache.util.EndpointExistsException;
import com.gemstone.gemfire.cache.util.EndpointInUseException;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.internal.cache.PoolManagerImpl;
import com.gemstone.gemfire.internal.cache.tier.ConnectionProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.AcceptorImpl;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * A pool for use by the old BridgeLoader/BridgeWriter.
 * This class can go away once we drop these deprecated classes
 * 
 * @author darrel
 * @since 5.7
 */
@SuppressWarnings("deprecation")
public class BridgePoolImpl extends PoolImpl implements ConnectionProxy {
  private static final Logger logger = LogService.getLogger();
  
  private static final AtomicInteger ID_COUNTER = new AtomicInteger();
  public static final int DEFAULT_CONNECTIONSPERSERVER = 1;
  public static final int DEFAULT_HANDSHAKE_TIMEOUT = AcceptorImpl.DEFAULT_HANDSHAKE_TIMEOUT_MS;
  public static final String DEFAULT_LBPOLICY = LBPolicy.STICKY_PROPERTY_NAME;
  
  //this field is only set to true when the cache is closing with
  //keep alive set to true.
  private boolean keepAlive;

  private static int getBridgePoolId() {
    return ID_COUNTER.incrementAndGet();
  }
  
  public static BridgePoolImpl create(Properties props, boolean usedByBridgeWriter) {
    return create(props, usedByBridgeWriter, false/*usedByGateway*/);
  }

  public static BridgePoolImpl create(Properties props, boolean usedByBridgeWriter,
                                      boolean usedByGateway) {
    
    String name = (usedByGateway ? "GatewayPool-" : "BridgePool-") + getBridgePoolId();
    PoolFactoryImpl pf = (PoolFactoryImpl)PoolManager.createFactory();
    pf.init(props, usedByBridgeWriter, usedByGateway);
    BridgePoolImpl result = (BridgePoolImpl)pf.create(name);
    if (!result.isDestroyed()) {
      result.attach();
    }
    return result;
  }
  /**
   * Should only be called by PoolFactoryImpl#create.
   * All other creators should use the static create method
   */
  public BridgePoolImpl(PoolManagerImpl pm, String name, Pool attributes) {
    super(pm, name, attributes);
    finishCreate(pm); // do this last since we are escaping the constructor
  }
  public static void loadEmergencyClasses() {
    // nyi
  }

  ////////////////// ConnectionProxy methods ////////////////////
  /**
   * Initializes this <code>ConnectionProxy</code> according to the
   * given properties.
   */
  public void initialize(Properties p) {
    throw new IllegalStateException("nyi");
  }



  public void finalizeProxy() {
    detach();
    if (getAttachCount() <= 0) {
      destroy(keepAlive);
    }
  }


  /**
   * Returns the load balancing policy in effect for this connection
   * proxy.
   */
  public String getLBPolicy() {
    if (getThreadLocalConnections()) {
      return "Sticky";
    } else {
      return "RoundRobin";
    }
  }


  /**
   * Returns the number of milliseconds to wait before re-connecting
   * to a dead server.
   */
  public int getRetryInterval() {
    return (int)getPingInterval();
  }


  /**
   * Closes this connection proxy and all of its connections
   */
  public void close() {
    if (logger.isDebugEnabled()) {
      logger.debug("BridgePoolImpl - closing");
    }
    finalizeProxy();
  }


  /**
   * Returned true if this ConnectionProxy has been initialized and not closed.
   */
  public boolean isOpen() {
    return !isDestroyed();
  }


  /**
   * Update bookkeeping on this proxy associated with the loss of a region.
   * In particular, remove all region interests.
   */
  public void detachRegion(Region r) {
    // nyi
  }


  /**
   * Returns the number of {@link Connection}s that should be created
   * to every cache server.
   */
  public int getConnectionsPerServer() {
    return getMinConnections();
  }


  /**
   * Notes that the server with the given name is unavailable
   */
  public void setServerUnavailable(String name) {
    throw new IllegalStateException("nyi");
  }


  /**
   * Notes that the server with the given name is available
   */
  public void setServerAvailable(String name) {
    throw new IllegalStateException("nyi");
  }


  /**
   * Stops this connection proxy and
   */
  public void terminate() {
    finalizeProxy();
  }


  /**
   * Releases the connection associated with the current thread
   */
  public void release() {
    // nyi
  }

  /**
   * Returns value of establishCallbackConnection property.
   * @since 4.2.3
   */
  public boolean getEstablishCallbackConnection() {
    return getSubscriptionEnabled();
  }


  /**
   * Add an <code>Endpoint</code> to the known <code>Endpoint</code>s.
   *
   * @param name The name of the endpoint to add
   * @param host The host name or ip address of the endpoint to add
   * @param port The port of the endpoint to add
   *
   * @throws EndpointExistsException if the <code>Endpoint</code> to be
   * added already exists.
   *
   * @since 5.0.2
   */
  public void addEndpoint(String name, String host, int port)
  throws EndpointExistsException {
    ((ExplicitConnectionSourceImpl)getConnectionSource()).addEndpoint(host,port);
  }


  /**
   * Remove an <code>Endpoint</code> from the dead <code>Endpoint</code>s.
   * The specified <code>Endpoint</code> must be dead.
   *
   * @param name The name of the endpoint to remove
   * @param host The host name or ip address of the endpoint to remove
   * @param port The port of the endpoint to remove
   *
   * @throws EndpointDoesNotExistException if the <code>Endpoint</code> to be
   * removed doesn't exist.
   *
   * @since 5.0.2
   */
  public void removeEndpoint(String name, String host, int port)
  throws EndpointDoesNotExistException,EndpointInUseException {
    ((ExplicitConnectionSourceImpl)getConnectionSource()).removeEndpoint(host,port);
  }


  /**
   * @return Returns the redundancy number
   * @since 5.1
   */
  public int getRedundancyLevel() {
    return getSubscriptionRedundancy();
  }

  /**
   * The configurable expiry time of last received sequence ID
   *
   * @return The configurable expiry time of last received sequence ID
   */
  public long getMessageTrackingTimeout() {
    return getSubscriptionMessageTrackingTimeout();
  }

  public void reuse() {
    attach();
  }
  
  
  public static boolean isLoaderOp(Op op) {
    return op instanceof GetOpImpl;
  }

  private RuntimeException transformException(RuntimeException ex, Op op) {
    if(isLoaderOp(op)) {
      if (ex instanceof SubscriptionNotEnabledException) {
        return new CacheLoaderException("establishCallbackConnection must be set to true", ex);
      } else if (ex instanceof CacheLoaderException) {
        return ex;
      } else if (ex instanceof CancelException) {
        return ex;
      } else if (ex instanceof ServerConnectivityException && ex.getCause() != null) {
        return new CacheLoaderException(ex.getCause());
      } else {
        return new CacheLoaderException(ex);
      }
    }
    else {
      if (ex instanceof SubscriptionNotEnabledException) {
        return new BridgeWriterException("establishCallbackConnection must be set to true", ex);
      } else if (ex instanceof CacheWriterException) {
        return ex;
      } else if (ex instanceof CancelException) {
        return ex;
      } else if (ex instanceof ServerConnectivityException && ex.getCause() != null) {
        return new BridgeWriterException(ex.getCause());
      } else {
        return new BridgeWriterException(ex);
      }
    }
  }
  
  @Override
  public Object execute(Op op) {
    try {
      return super.execute(op);
    } catch (RuntimeException ex) {
      throw transformException(ex, op);
    }
  }

  @Override
  public Object executeOn(ServerLocation server, Op op) {
    try {
      return super.executeOn(server, op);
    } catch (RuntimeException ex) {
      throw transformException(ex, op);
    }
  }
  
  @Override
  public Object executeOn(Connection con, Op op) {
    try {
      return super.executeOn(con, op);
    } catch (RuntimeException ex) {
      throw transformException(ex, op);
    }
  }

  @Override
  public Object executeOn(Connection con, Op op, boolean timeoutFatal) {
    try {
      return super.executeOn(con, op, timeoutFatal);
    } catch (RuntimeException ex) {
      throw transformException(ex, op);
    }
  }

  @Override
  public Object executeOnQueuesAndReturnPrimaryResult(Op op) {
    try {
      return super.executeOnQueuesAndReturnPrimaryResult(op);
    } catch (RuntimeException ex) {
      throw transformException(ex, op);
    }
  }

  @Override
  public void executeOnAllQueueServers(Op op)
    throws NoSubscriptionServersAvailableException, SubscriptionNotEnabledException {
    try {
      super.executeOnAllQueueServers(op);
    } catch (RuntimeException ex) {
      throw transformException(ex, op);
    }
  }

  @Override
  public Object executeOnPrimary(Op op) {
    try {
      return super.executeOnPrimary(op);
    } catch (RuntimeException ex) {
      throw transformException(ex, op);
    }
  }
  public void setKeepAlive(boolean keepAlive) {
    this.keepAlive = keepAlive;
  }
  
  /** ******** INNER CLASSES ************************* */
  public static class LBPolicy
  {
    public static final String STICKY_PROPERTY_NAME = "Sticky";

    public static final String RANDOMSTICKY_PROPERTY_NAME = "RandomSticky";

    public static final String ROUNDROBIN_PROPERTY_NAME = "RoundRobin";

    public static final String RANDOM_PROPERTY_NAME = "Random";

    public static final String APPASSISTED_PROPERTY_NAME = "AppAssisted";

    public static final int STICKY = 0;

    public static final int ROUNDROBIN = 1;

    public static final int APPASSISTED = 2;

    public static final int RANDOM = 3;

    public static final int RANDOMSTICKY = 4;

    public final int thePolicy;

    public LBPolicy(String name) {
      if (name.equalsIgnoreCase(STICKY_PROPERTY_NAME)) {
        this.thePolicy = STICKY;
      }
      else if (name.equalsIgnoreCase(ROUNDROBIN_PROPERTY_NAME)) {
        this.thePolicy = ROUNDROBIN;
      }
      else if (name.equalsIgnoreCase(APPASSISTED_PROPERTY_NAME)) {
        this.thePolicy = APPASSISTED;
      }
      else if (name.equalsIgnoreCase(RANDOM_PROPERTY_NAME)) {
        this.thePolicy = RANDOM;
      }
      else if (name.equalsIgnoreCase(RANDOMSTICKY_PROPERTY_NAME)) {
        this.thePolicy = RANDOMSTICKY;
      }
      else {
        this.thePolicy = STICKY; // DEFAULT
      }
    }

    public int getPolicy()
    {
      return this.thePolicy;
    }

    public boolean isSticky() {
      return getPolicy() == STICKY || getPolicy() == RANDOMSTICKY;
    }

    public boolean isRandom() {
      return getPolicy() == RANDOM || getPolicy() == RANDOMSTICKY;
    }

    public String getPolicyPropertyName(int pol)
    {
      String retStr;
      switch (pol) {
      case STICKY:
        retStr = STICKY_PROPERTY_NAME;
        break;
      case ROUNDROBIN:
        retStr = ROUNDROBIN_PROPERTY_NAME;
        break;
      case APPASSISTED:
        retStr = APPASSISTED_PROPERTY_NAME;
        break;
      case RANDOM:
        retStr = RANDOM_PROPERTY_NAME;
        break;
      case RANDOMSTICKY:
        retStr = RANDOMSTICKY_PROPERTY_NAME;
        break;
      default:
        return Integer.toString(pol);
      }
      return retStr;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == this) {
        return true;
      }

      if (obj instanceof LBPolicy) {
        LBPolicy other = (LBPolicy)obj;
        return this.thePolicy == other.thePolicy;
      }
      else {
        return false;
      }
    }

    @Override
    public int hashCode()
    {
      return this.thePolicy;
    }

    @Override
    public String toString()
    {
      return getPolicyPropertyName(this.thePolicy);
    }
  }

  
}
