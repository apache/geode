/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.management.membership;

/**
 * Utility class that implements all methods in
 * <code>ClientMembershipListener</code> with empty implementations.
 * Applications can subclass this class and only override the methods for the
 * events of interest.
 *
 * @author rishim
 * @since 8.0
 */
public abstract class ClientMembershipListenerAdapter implements ClientMembershipListener {

  /**
   * Invoked when a client has connected to this process or when this process
   * has connected to a CacheServer.
   */
  public void memberJoined(ClientMembershipEvent event) {
  }

  /**
   * Invoked when a client has gracefully disconnected from this process or when
   * this process has gracefully disconnected from a CacheServer.
   */
  public void memberLeft(ClientMembershipEvent event) {
  }

  /**
   * Invoked when a client has unexpectedly disconnected from this process or
   * when this process has unexpectedly disconnected from a CacheServer.
   */
  public void memberCrashed(ClientMembershipEvent event) {
  }

}
