/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.admin;

import org.apache.geode.cache.*;

/**
 * A listener whose callback methods can be used to track the lifecycle of
 * {@link Cache caches} and {@link Region regions} in the GemFire distributed system.
 *
 * @see AdminDistributedSystem#addCacheListener
 * @see AdminDistributedSystem#removeCacheListener
 *
 * @since GemFire 5.0
 * @deprecated as of 7.0 use the <code><a href="{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code> package instead
 */
public interface SystemMemberCacheListener {

  /**
   * Invoked after a region is created in any node of distributed system.
   * @param event describes the region that was created.
   * @see CacheFactory#create
   * @see Cache#createRegion
   * @see Region#createSubregion
   */
  public void afterRegionCreate(SystemMemberRegionEvent event);

  /**
   * Invoked when a region is destroyed or closed in any node of distributed system.
   * @param event describes the region that was lost. The operation on this event
   * can be used to determine the actual operation that caused the loss. Note that
   * {@link Cache#close()} invokes this callback with <code>Operation.CACHE_CLOSE</code>
   * for each region in the closed cache and it invokes {@link #afterCacheClose}.
   
   * @see Cache#close()
   * @see Region#close
   * @see Region#localDestroyRegion()
   * @see Region#destroyRegion()
   */
  public void afterRegionLoss(SystemMemberRegionEvent event);

  /**
   * Invoked after a cache is created in any node of a distributed system.
   * Note that this callback will be done before any regions are created in the
   * cache.
   * @param event describes the member that created the cache.
   * @see CacheFactory#create
   */
  public void afterCacheCreate(SystemMemberCacheEvent event);
  /**
   * Invoked after a cache is closed in any node of a distributed system.
   * This callback is done after those done for each region in the cache.
   * This callback is not done if the distributed member that has a cache crashes.
   * @param event describes the member that closed its cache.
   * @see Cache#close()
   */
  public void afterCacheClose(SystemMemberCacheEvent event);
}
