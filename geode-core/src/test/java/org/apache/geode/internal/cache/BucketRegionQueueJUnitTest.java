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

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderHelper;
import org.apache.geode.test.fake.Fakes;

public class BucketRegionQueueJUnitTest {

  private static final String GATEWAY_SENDER_ID = "ny";
  private static final int BUCKET_ID = 85;
  private static final long KEY = 198;

  private GemFireCacheImpl cache;
  private PartitionedRegion queueRegion;
  private AbstractGatewaySender sender;
  private PartitionedRegion rootRegion;
  private BucketRegionQueue bucketRegionQueue;

  @Before
  public void setUpGemFire() {
    createCache();
    createQueueRegion();
    createGatewaySender();
    createRootRegion();
    createBucketRegionQueue();
  }

  private void createCache() {
    // Mock cache
    this.cache = Fakes.cache();
  }

  private void createQueueRegion() {
    // Mock queue region
    this.queueRegion =
        ParallelGatewaySenderHelper.createMockQueueRegion(this.cache,
            ParallelGatewaySenderHelper.getRegionQueueName(GATEWAY_SENDER_ID));
  }

  private void createGatewaySender() {
    // Mock gateway sender
    this.sender = ParallelGatewaySenderHelper.createGatewaySender(this.cache);
    when(this.queueRegion.getParallelGatewaySender()).thenReturn(this.sender);
  }

  private void createRootRegion() {
    // Mock root region
    this.rootRegion = mock(PartitionedRegion.class);
    when(this.rootRegion.getFullPath())
        .thenReturn(Region.SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME);
  }

  private void createBucketRegionQueue() {
    BucketRegionQueue realBucketRegionQueue = ParallelGatewaySenderHelper
        .createBucketRegionQueue(this.cache, this.rootRegion, this.queueRegion, BUCKET_ID);
    this.bucketRegionQueue = spy(realBucketRegionQueue);
  }

  @Test
  public void testBasicDestroyConflationEnabledAndValueInRegionAndIndex() {
    // Create the event
    EntryEventImpl event = EntryEventImpl.create(this.bucketRegionQueue, Operation.DESTROY,
        KEY, "value", null, false, mock(DistributedMember.class));

    // Don't allow hasSeenEvent to be invoked
    doReturn(false).when(this.bucketRegionQueue).hasSeenEvent(event);

    // Set conflation enabled and the appropriate return values for containsKey and removeIndex
    when(this.queueRegion.isConflationEnabled()).thenReturn(true);
    when(this.bucketRegionQueue.containsKey(KEY)).thenReturn(true);
    doReturn(true).when(this.bucketRegionQueue).removeIndex(KEY);

    // Invoke basicDestroy
    this.bucketRegionQueue.basicDestroy(event, true, null);

    // Verify mapDestroy is invoked
    verify(this.bucketRegionQueue).mapDestroy(event, true, false, null);
  }

  @Test(expected = EntryNotFoundException.class)
  public void testBasicDestroyConflationEnabledAndValueNotInRegion() {
    // Create the event
    EntryEventImpl event = EntryEventImpl.create(this.bucketRegionQueue, Operation.DESTROY,
        KEY, "value", null, false, mock(DistributedMember.class));

    // Don't allow hasSeenEvent to be invoked
    doReturn(false).when(this.bucketRegionQueue).hasSeenEvent(event);

    // Set conflation enabled and the appropriate return values for containsKey and removeIndex
    when(this.queueRegion.isConflationEnabled()).thenReturn(true);
    when(this.bucketRegionQueue.containsKey(KEY)).thenReturn(false);

    // Invoke basicDestroy
    this.bucketRegionQueue.basicDestroy(event, true, null);
  }
}
