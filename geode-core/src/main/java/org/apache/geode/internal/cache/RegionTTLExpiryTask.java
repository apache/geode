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

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.ExpirationAction;

class RegionTTLExpiryTask extends RegionExpiryTask {

  /** Creates a new instance of RegionTTLExpiryTask */
  RegionTTLExpiryTask(LocalRegion reg) {
    super(reg);
  }

  /**
   * @return the absolute time (ms since Jan 1, 1970) at which this region expires, due to either
   *         time-to-live or idle-timeout (whichever will occur first), or 0 if neither are used.
   */
  @Override
  public long getExpirationTime() throws EntryNotFoundException {
    // if this is an invalidate action and region has already been invalidated,
    // then don't expire again until the full timeout from now.
    ExpirationAction action = getAction();
    if (action == ExpirationAction.INVALIDATE || action == ExpirationAction.LOCAL_INVALIDATE) {
      if (getLocalRegion().isRegionInvalid()) {
        int timeout = getTTLAttributes().getTimeout();
        if (timeout == 0) {
          return 0L;
        }
        if (!getLocalRegion().EXPIRY_UNITS_MS) {
          timeout *= 1000;
        }
        // Sometimes region expiration depends on lastModifiedTime which in turn
        // depends on entry modification time. To make it consistent always use
        // cache time here.
        return timeout + getLocalRegion().cacheTimeMillis();
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
  protected void addExpiryTask() {
    getLocalRegion().addTTLExpiryTask(this);
  }

  @Override
  public boolean isPending() {
    return false;
  }
}
