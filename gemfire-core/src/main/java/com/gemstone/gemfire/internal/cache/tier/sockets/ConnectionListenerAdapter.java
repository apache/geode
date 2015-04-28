/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

/**
 * A ConnectionListener that does nothing.
 * @author dsmith
 * @since 5.7
 *
 */
public class ConnectionListenerAdapter implements ConnectionListener {

  public void connectionClosed(boolean lastConnection, byte communicationMode) {
  }

  public void connectionOpened(boolean firstConnection, byte communicationMode) {
  }

  public void queueAdded(ClientProxyMembershipID id) {
  }

  public void queueRemoved() {
  }
}
