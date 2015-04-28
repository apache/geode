/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.internal.cache.tier.InternalBridgeMembership;

/**
 * Provides utility methods for registering and unregistering
 * BridgeMembershipListeners in this process.
 *
 * @author Kirk Lund
 * @since 4.2.1
 * @deprecated see com.gemstone.gemfire.management.membership.ClientMembership
 */
public final class BridgeMembership {

  private BridgeMembership() {}

  /**
   * Registers a {@link BridgeMembershipListener} for notification of
   * connection changes for BridgeServers and bridge clients.
   * @param listener a BridgeMembershipListener to be registered
   */
  public static void registerBridgeMembershipListener(BridgeMembershipListener listener) {
    InternalBridgeMembership.registerBridgeMembershipListener(listener);
  }

  /**
   * Removes registration of a previously registered {@link
   * BridgeMembershipListener}.
   * @param listener a BridgeMembershipListener to be unregistered
   */
  public static void unregisterBridgeMembershipListener(BridgeMembershipListener listener) {
    InternalBridgeMembership.unregisterBridgeMembershipListener(listener);
  }

  /**
   * Returns an array of all the currently registered
   * <code>BridgeMembershipListener</code>s. Modifications to the returned
   * array will not effect the registration of these listeners.
   * @return the registered <code>BridgeMembershipListener</code>s; an empty
   * array if no listeners
   */
  public static BridgeMembershipListener[] getBridgeMembershipListeners() {
    return InternalBridgeMembership.getBridgeMembershipListeners();
  }


}

