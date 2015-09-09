/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.*;

/**
 *
 * @author Eric Zoerner
 */
class RegionIdleExpiryTask extends RegionExpiryTask {
  
  /** Creates a new instance of RegionIdleExpiryTask */
  RegionIdleExpiryTask(LocalRegion reg) {
    super(reg);
  }

  /**
   * @return the absolute time (ms since Jan 1, 1970) at which this
   * region expires, due to either time-to-live or idle-timeout (whichever
   * will occur first), or 0 if neither are used.
   */
  @Override
  public long getExpirationTime() throws EntryNotFoundException {
    // if this is an invalidate action and region has already been invalidated,
    // then don't expire again until the full timeout from now.
    ExpirationAction action = getAction();
    if (action == ExpirationAction.INVALIDATE ||
        action == ExpirationAction.LOCAL_INVALIDATE) {
      if (getLocalRegion().regionInvalid) {
        int timeout = getIdleAttributes().getTimeout();
        if (timeout == 0) return 0L;
        if (!getLocalRegion().EXPIRY_UNITS_MS) {
          timeout *= 1000;
        }
        // Expiration should always use the DSClock instead of the System clock.
        return  timeout + getLocalRegion().cacheTimeMillis();
      }
    }
    // otherwise, expire at timeout plus last accessed time
    return getIdleExpirationTime();
  }
  
  @Override
  protected ExpirationAction getAction() {
    return getIdleAttributes().getAction();
  }
  
  @Override
  protected final void addExpiryTask() {
    getLocalRegion().addIdleExpiryTask(this);
  }

  @Override
  public boolean isPending() {
    return false;
  }
}
