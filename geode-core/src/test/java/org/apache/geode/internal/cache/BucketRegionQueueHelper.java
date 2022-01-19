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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;

/**
 * Helper class in the internal cache package to access protected BucketRegionQueue methods.
 */
public class BucketRegionQueueHelper {

  private final BucketRegionQueue bucketRegionQueue;

  public BucketRegionQueueHelper(GemFireCacheImpl cache, PartitionedRegion queueRegion,
      BucketRegionQueue bucketRegionQueue) {
    this.bucketRegionQueue = bucketRegionQueue;
    initialize(cache, queueRegion);
  }

  public GatewaySenderEventImpl addEvent(Object key) {
    bucketRegionQueue.getEventTracker().setInitialized();
    bucketRegionQueue.entries.disableLruUpdateCallback();
    GatewaySenderEventImpl event = mock(GatewaySenderEventImpl.class);
    bucketRegionQueue.entries.initialImagePut(key, 0, event, false, false, null, null, false);
    bucketRegionQueue.entries.enableLruUpdateCallback();
    return event;
  }

  public void cleanUpDestroyedTokensAndMarkGIIComplete() {
    bucketRegionQueue
        .cleanUpDestroyedTokensAndMarkGIIComplete(InitialImageOperation.GIIStatus.NO_GII);
  }

  public void initialize(GemFireCacheImpl cache, PartitionedRegion queueRegion) {
    InternalDistributedMember member = cache.getMyId();
    when(queueRegion.getMyId()).thenReturn(member);
    when(cache.getInternalRegionByPath(bucketRegionQueue.getFullPath()))
        .thenReturn(bucketRegionQueue);
  }
}
