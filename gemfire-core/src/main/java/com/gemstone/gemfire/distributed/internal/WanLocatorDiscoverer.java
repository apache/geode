package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.cache.client.internal.locator.wan.LocatorMembershipListener;

public interface WanLocatorDiscoverer {
  
  public static final int WAN_LOCATOR_CONNECTION_TIMEOUT = Integer.getInteger(
      "WANLocator.CONNECTION_TIMEOUT", 50000).intValue();
  
  /**
   * For WAN 70 Exchange the locator information within the distributed system
   */
  void discover(int port, DistributionConfigImpl config,
      LocatorMembershipListener locatorListener);
}
