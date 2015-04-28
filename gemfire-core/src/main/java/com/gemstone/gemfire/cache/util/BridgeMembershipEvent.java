/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.management.membership.ClientMembershipEvent;

/**
 * An event delivered to a {@link BridgeMembershipListener} when this 
 * process detects connection changes to BridgeServers or bridge clients.
 *
 * @author Kirk Lund
 * @since 4.2.1
 * @deprecated see com.gemstone.gemfire.management.membership.ClientMembershipEvent
 */
public interface BridgeMembershipEvent extends ClientMembershipEvent {
  
}

