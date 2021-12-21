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
package org.apache.geode.internal.cache.tier;

import org.apache.geode.CancelException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;

/**
 * Helper class that maintains a weak hashmap of referenced regions
 *
 * @since GemFire 2.0.2
 */
public class CachedRegionHelper {

  private final InternalCache cache;
  private final InternalCache realCache;

  private volatile boolean shutdown = false;

  public CachedRegionHelper(InternalCache cache) {
    realCache = cache;
    this.cache = new InternalCacheForClientAccess(cache);
  }

  public void checkCancelInProgress(Throwable e) throws CancelException {
    cache.getCancelCriterion().checkCancelInProgress(e);
  }

  public Region getRegion(String name) {
    return cache.getRegion(name);
  }

  public InternalCache getCache() {
    return cache;
  }

  public InternalCache getCacheForGatewayCommand() {
    return realCache;
  }

  public void setShutdown(boolean shutdown) {
    this.shutdown = shutdown;
  }

  public boolean isShutdown() {
    return shutdown || cache.getCancelCriterion().isCancelInProgress();
  }

  /**
   * CachedRegionHelper#close() does nothing
   */
  public void close() {
    // cache = null;
  }

}
