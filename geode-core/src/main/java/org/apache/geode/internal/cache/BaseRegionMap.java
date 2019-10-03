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

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.eviction.EvictableEntry;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.logging.internal.LogService;

/**
 * BaseRegionMap consolidates common behaviour between {@link AbstractRegionMap} and
 * {@link ProxyRegionMap}. {@link AbstractRegionMap} should be extended when implementing
 * a new {@link RegionMap} and {@link ProxyRegionMap} should be used if an empty {@link RegionMap}
 * for regions whose DataPolicy is Proxy is required.
 */
abstract class BaseRegionMap implements RegionMap {
  private static final Logger logger = LogService.getLogger();

  @Override
  public void incRecentlyUsed() {
    // nothing
  }

  @Override
  public long getEvictions() {
    return 0;
  }

  @Override
  public EvictionController getEvictionController() {
    return null;
  }

  @Override
  public void lruUpdateCallback() {
    // nothing needed
  }

  @Override
  public boolean lruLimitExceeded(DiskRegionView diskRegionView) {
    return false;
  }

  @Override
  public void lruCloseStats() {
    // nothing needed
  }

  @Override
  public void resetThreadLocals() {
    // By default do nothing; LRU maps needs to override this method
  }


  @Override
  public boolean disableLruUpdateCallback() {
    // nothing needed
    return false;
  }

  @Override
  public void enableLruUpdateCallback() {
    // By default do nothing; LRU maps needs to override this method
  }

  @Override
  public int centralizedLruUpdateCallback() {
    return 0;
  }

  @Override
  public void updateEvictionCounter() {}

  @Override
  public void finishChangeValueForm() {}

  @Override
  public boolean beginChangeValueForm(EvictableEntry le,
      CachedDeserializable vmCachedDeserializable, Object v) {
    return false;
  }

  @Override
  public void lruEntryFaultIn(EvictableEntry entry) {
    // do nothing by default
  }

  /**
   * If true then invalidates that throw EntryNotFoundException or that are already invalid will
   * first call afterInvalidate on CacheListeners. The old value on the event passed to
   * afterInvalidate will be null. If the region is not initialized then callbacks will not be done.
   * This property only applies to non-transactional invalidates. Transactional invalidates ignore
   * this property. Note that empty "proxy" regions on a client will not be sent invalidates from
   * the server unless they also set the proxy InterestPolicy to ALL. If the invalidate is not sent
   * then this property will not cause a listener on that client to be notified of the invalidate. A
   * non-empty "caching-proxy" will receive invalidates from the server.
   */
  @MutableForTesting
  public static boolean FORCE_INVALIDATE_EVENT =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "FORCE_INVALIDATE_EVENT");

  /**
   * If the FORCE_INVALIDATE_EVENT flag is true then invoke callbacks on the given event.
   */
  static void forceInvalidateEvent(EntryEventImpl event, LocalRegion owner) {
    if (FORCE_INVALIDATE_EVENT) {
      event.invokeCallbacks(owner, false, false);
    }
  }

  static boolean shouldInvokeCallbacks(final LocalRegion owner, final boolean isInitialized) {
    LocalRegion lr = owner;
    boolean isPartitioned = lr.isUsedForPartitionedRegionBucket();

    if (isPartitioned) {
      /*
       * if(!((BucketRegion)lr).getBucketAdvisor().isPrimary()) {
       * if(!BucketRegion.FORCE_LOCAL_LISTENERS_INVOCATION) { return false; } }
       */
      lr = owner.getPartitionedRegion();
    }
    return (isPartitioned || isInitialized) && (lr.shouldDispatchListenerEvent()
        || lr.shouldNotifyBridgeClients() || lr.getConcurrencyChecksEnabled());
  }

  /**
   * Switch the event's region from BucketRegion to owning PR and set originRemote to the given
   * value
   */
  static EntryEventImpl switchEventOwnerAndOriginRemote(EntryEventImpl event,
      boolean originRemote) {
    assert event != null;
    if (event.getRegion().isUsedForPartitionedRegionBucket()) {
      LocalRegion pr = event.getRegion().getPartitionedRegion();
      event.setRegion(pr);
    }
    event.setOriginRemote(originRemote);
    return event;
  }
}
