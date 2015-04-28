/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

/**
 * A listener which can be registered on {@link AcceptorImpl} 
 * in order to receive events about connections created
 * or destroyed for this acceptor.
 * @author dsmith
 * @since 5.7
 *
 */
public interface ConnectionListener {
  /**
   * Indicates that a new connection has been opened
   * to this acceptor.
   * @param firstConnection true if this is the first connection
   * from this client.
   * @param communicationMode the communication mode of this
   * connection.
   */
  void connectionOpened(boolean firstConnection, byte communicationMode);
  /**
   * Indicates that the a connection to this acceptor has been
   * closed.
   * @param lastConnection indicates that this was the last
   * connection from this client.
   * @param communicationMode of this connection. 
   */
  void connectionClosed(boolean lastConnection, byte communicationMode);
  
  /**
   * Indicates that a new queue was created on this Acceptor.
   */
  void queueAdded(ClientProxyMembershipID id);
  
  /**
   * Indicates that a queue was removed from this Acceptor.
   */
  void queueRemoved();
}
