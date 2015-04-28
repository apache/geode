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
class RegionTTLExpiryTask  extends RegionExpiryTask {
  
  /** Creates a new instance of RegionTTLExpiryTask */
  RegionTTLExpiryTask(LocalRegion reg) {
    super(reg);
  }

  /**
   * @return the absolute time (ms since Jan 1, 1970) at which this
   * region expires, due to either time-to-live or idle-timeout (whichever
   * will occur first), or 0 if neither are used.
   */
  @Override
  long getExpirationTime() throws EntryNotFoundException {
    // if this is an invalidate action and region has already been invalidated,
    // then don't expire again until the full timeout from now.
    ExpirationAction action = getAction();
    if (action == ExpirationAction.INVALIDATE ||
        action == ExpirationAction.LOCAL_INVALIDATE) {
      if (getLocalRegion().regionInvalid) {
        int timeout = getTTLAttributes().getTimeout();
        if (timeout == 0) return 0L;
        if (!getLocalRegion().EXPIRY_UNITS_MS) {
          timeout *= 1000;
        }
        // Sometimes region expiration depends on lastModifiedTime which in turn 
        // depends on entry modification time. To make it consistent always use
        // cache time here.
        return  timeout + getLocalRegion().cacheTimeMillis();
      }
    }
    // otherwise, expire at timeout plus last modified time
    return getTTLExpirationTime();
  }
  
  @Override
  protected ExpirationAction getAction() {
    return getTTLAttributes().getAction();
  }
  
  @Override
  protected final void addExpiryTask() {
    getLocalRegion().addTTLExpiryTask(this);
  }

  @Override
  public boolean isPending() {
    return false;
  }
}
