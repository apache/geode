/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.membership;

import com.gemstone.gemfire.internal.cache.tier.InternalClientMembership;

/**
 * Provides utility methods for registering and unregistering
 * ClientMembershipListeners in this process.
 *
 * @author rishim
 * @since 8.0
 */
public final class ClientMembership {

  private ClientMembership() {
  }

  /**
   * Registers a {@link ClientMembershipListener} for notification of connection
   * changes for CacheServer and clients.
   * 
   * @param listener
   *          a ClientMembershipListener to be registered
   */
  public static void registerClientMembershipListener(ClientMembershipListener listener) {
    InternalClientMembership.registerClientMembershipListener(listener);
  }

  /**
   * Removes registration of a previously registered
   * {@link ClientMembershipListener}.
   * 
   * @param listener
   *          a ClientMembershipListener to be unregistered
   */
  public static void unregisterClientMembershipListener(ClientMembershipListener listener) {
    InternalClientMembership.unregisterClientMembershipListener(listener);
  }

  /**
   * Returns an array of all the currently registered
   * <code>ClientMembershipListener</code>s. Modifications to the returned array
   * will not affect the registration of these listeners.
   * 
   * @return the registered <code>ClientMembershipListener</code>s; an empty
   *         array if no listeners
   */
  public static ClientMembershipListener[] getClientMembershipListeners() {
    return InternalClientMembership.getClientMembershipListeners();
  }

}
