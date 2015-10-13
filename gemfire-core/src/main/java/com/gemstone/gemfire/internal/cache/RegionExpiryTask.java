/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

/**
 * RegionExpiryTask represents a timeout event for region expiration
 */

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.TimeoutException;

abstract class RegionExpiryTask extends ExpiryTask
  {
  private boolean isCanceled;

  protected RegionExpiryTask(LocalRegion reg) {
    super(reg);
    this.isCanceled = false;
  }

  @Override
  public Object getKey() {
    return null;
  }
  
  @Override
  protected ExpirationAttributes getIdleAttributes() {
    return getLocalRegion().getRegionIdleTimeout();
  }
  @Override
  protected ExpirationAttributes getTTLAttributes() {
    return getLocalRegion().getRegionTimeToLive();
  }

  @Override
  protected final long getLastAccessedTime()
  {
    return getLocalRegion().getLastAccessedTime();
  }

  @Override
  protected final long getLastModifiedTime()
  {
    return getLocalRegion().getLastModifiedTime();
  }

  @Override
  protected final boolean destroy(boolean isPending) throws CacheException
  {
    return getLocalRegion().expireRegion(this, true, true);
  }

  @Override
  protected final boolean invalidate() throws TimeoutException
  {
    return getLocalRegion().expireRegion(this, true, false);
  }

  @Override
  protected final boolean localDestroy() throws CacheException
  {
    return getLocalRegion().expireRegion(this, false, true);
  }

  @Override
  protected final boolean localInvalidate()
  {
    return getLocalRegion().expireRegion(this, false, false);
  }

  @Override
  public boolean cancel()
  {
    isCanceled = true;
    return super.cancel();
  }

  @Override
  protected final void performTimeout() throws CacheException
  {
    if (isCanceled) {
      return;
    }
    super.performTimeout();
  }

  @Override
  protected final void basicPerformTimeout(boolean isPending) throws CacheException
  {
    if (isCanceled) {
      return;
    }
    if (!isExpirationAllowed()) {
      return;
    }
    if (isExpirationPossible()) {
      if (expire(isPending)) {
        reschedule();
      }
    } else {
      reschedule();
    }
  }

  @Override
  final protected void reschedule() throws CacheException
  {
    if (isCacheClosing() || getLocalRegion().isClosed() || getLocalRegion().isDestroyed()
        || !isExpirationAllowed()) {
      return;
    }

    addExpiryTask();
    if (expiryTaskListener != null) {
      expiryTaskListener.afterReschedule(this);
    }
  }

  @Override
  public String toString()
  {
    String expireTime = "<unavailable>";
    try {
      expireTime = String.valueOf(getExpirationTime());
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
    }
    return super.toString() + " for " + getLocalRegion().getFullPath()
        + ", expiration time: " + expireTime + " [now: "
 + getNow() + "]";
  }
}
