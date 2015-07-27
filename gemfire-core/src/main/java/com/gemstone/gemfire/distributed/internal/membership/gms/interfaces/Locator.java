package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import com.gemstone.gemfire.distributed.internal.membership.NetView;

/**
 * The Locator interface allows member services to interact with the
 * Locator TcpHandler component of Geode's locator.  The Locator
 * handler's lifecycle is not controlled by member services.
 */
public interface Locator {
  /**
   * called when a new view is installed by Membership
   */
  void installView(NetView v);

}
