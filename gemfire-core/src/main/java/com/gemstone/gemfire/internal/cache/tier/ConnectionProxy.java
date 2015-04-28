/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier;

import java.util.*;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.util.EndpointDoesNotExistException;
import com.gemstone.gemfire.cache.util.EndpointExistsException;
import com.gemstone.gemfire.cache.util.EndpointInUseException;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * Defines the connection proxy interface, implementations of which
 * provide connection management facilities to the bridge loader. 
 *
 * @author Sudhir Menon
 * @since 2.0.2
 */
@SuppressWarnings("deprecation")
public interface ConnectionProxy {

/**
   * The GFE version of the client.
   * @since 5.7
   */
  public static final Version VERSION = Version.CURRENT.getGemFireVersion();

  public abstract void finalizeProxy();

  /**
   * Returns the load balancing policy in effect for this connection
   * proxy.
   */
  public abstract String getLBPolicy();

  /**
   * Returns the number of milliseconds to wait before re-connecting
   * to a dead server.
   */
  public abstract int getRetryInterval();

  /**
   * Returns the number of milliseconds to wait before timing out
   * client/server communication.
   */
  public abstract int getReadTimeout();


  /**
   * Closes this connection proxy and all of its connections
   */
  public abstract void close();

  /**
   * Returned true if this ConnectionProxy has been initialized and not closed.
   */
  public abstract boolean isOpen();

  /**
   * Update bookkeeping on this proxy associated with the loss of a region.
   * In particular, remove all region interests.
   */
  public abstract void detachRegion(Region r);

  /**
   * Returns the number of connections that should be created
   * to every cache server.
   */
  public abstract int getConnectionsPerServer();

  /**
   * Notes that the server with the given name is unavailable
   */
  public abstract void setServerUnavailable(String name);

  /**
   * Notes that the server with the given name is available
   */
  public abstract void setServerAvailable(String name);

  /**
   * Stops this connection proxy and
   */
  public abstract void terminate();

  /**
   * Releases the connection associated with the current thread
   */
  public abstract void release();

  /**
   * Returns value of establishCallbackConnection property.
   * @since 4.2.3
   */
  public boolean getEstablishCallbackConnection();

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
  throws EndpointExistsException;

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
   * @throws EndpointInUseException if the <code>Endpoint</code> to be removed
   * contains <code>Connection</code>s
   *
   * @since 5.0.2
   */
  public void removeEndpoint(String name, String host, int port)
  throws EndpointDoesNotExistException, EndpointInUseException;

  /**
   * @return Returns the threadIdToSequenceId.
   * @since 5.1
   */
  public Map getThreadIdToSequenceIdMap();

  /**
   * Verify if this EventId is already present in the map or not. If it is
   * already present then return true
   *
   * @param eventId the EventId of the incoming event
   * @return true if it is already present
   * @since 5.1
   */
  public abstract boolean verifyIfDuplicate(EventID eventId, boolean addToMap);

  /**
   * @return Returns the redundancy number
   * @since 5.1
   */
  public int getRedundancyLevel();

  /**
   * Returns the cancellation criterion for this proxy
   * @return the cancellation criterion
   */
  public CancelCriterion getCancelCriterion();

  /**
   * The configurable expiry time of last received sequence ID
   *
   * @return The configurable expiry time of last received sequence ID
   */
  public long getMessageTrackingTimeout();

  public boolean isDurableClient();

  public void reuse();
  public int getRetryAttempts();

  /**
   * Test hook for getting the client proxy membership id from this proxy.
   */
  public ClientProxyMembershipID getProxyID();
}
