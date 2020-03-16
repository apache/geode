/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.cache;

/*
 * RegionExpiryTask represents a timeout event for region expiration
 */

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.TimeoutException;

abstract class RegionExpiryTask extends ExpiryTask {
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
  protected long getLastAccessedTime() {
    return getLocalRegion().getLastAccessedTime();
  }

  @Override
  protected long getLastModifiedTime() {
    return getLocalRegion().getLastModifiedTime();
  }

  @Override
  protected boolean destroy(boolean isPending) throws CacheException {
    return getLocalRegion().expireRegion(this, true, true);
  }

  @Override
  protected boolean invalidate() throws TimeoutException {
    return getLocalRegion().expireRegion(this, true, false);
  }

  @Override
  protected boolean localDestroy() throws CacheException {
    return getLocalRegion().expireRegion(this, false, true);
  }

  @Override
  protected boolean localInvalidate() {
    return getLocalRegion().expireRegion(this, false, false);
  }

  @Override
  public boolean cancel() {
    isCanceled = true;
    return super.cancel();
  }

  @Override
  protected void performTimeout() throws CacheException {
    if (isCanceled) {
      return;
    }
    super.performTimeout();
  }

  @Override
  protected void basicPerformTimeout(boolean isPending) throws CacheException {
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
  protected void reschedule() throws CacheException {
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
  public String toString() {
    String expireTime = "<unavailable>";
    try {
      expireTime = String.valueOf(getExpirationTime());
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable e) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
    }
    return super.toString() + " for " + getLocalRegion().getFullPath() + ", expiration time: "
        + expireTime + " [now: " + calculateNow() + "]";
  }
}
