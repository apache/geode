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
package org.apache.geode.internal.cache.wan;

import static junitparams.JUnitParamsRunner.$;
import static org.assertj.core.api.Assertions.assertThat;

import junitparams.Parameters;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.versions.VMVersionTag;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({AEQTest.class})
@RunWith(GeodeParamsRunner.class)
public class AsyncEventQueueEntrySynchronizationDUnitTest extends AsyncEventQueueTestBase {

  private static Object[] getRegionShortcuts() {
    return $(new Object[] {RegionShortcut.PARTITION_REDUNDANT, false, true},
        new Object[] {RegionShortcut.PARTITION_REDUNDANT_PERSISTENT, true, true});
  }

  @Test
  @Parameters(method = "getRegionShortcuts")
  public void testParallelAsyncEventQueueSynchronization(RegionShortcut regionShortcut,
      boolean isPersistentAeq, boolean isParallelAeq) throws Exception {
    // Start locator
    Integer locatorPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));

    // Start three members
    vm1.invoke(() -> createCache(locatorPort));
    vm2.invoke(() -> createCache(locatorPort));
    vm3.invoke(() -> createCache(locatorPort));

    // Create parallel AsyncEventQueue
    String aeqId = "db";
    vm1.invoke(() -> createAsyncEventQueue(aeqId, isParallelAeq, 100, 1, false, isPersistentAeq,
        null, true, new WaitingAsyncEventListener()));
    vm2.invoke(() -> createAsyncEventQueue(aeqId, isParallelAeq, 100, 1, false, isPersistentAeq,
        null, true, new WaitingAsyncEventListener()));
    vm3.invoke(() -> createAsyncEventQueue(aeqId, isParallelAeq, 100, 1, false, isPersistentAeq,
        null, true, new WaitingAsyncEventListener()));

    // Create PartitionedRegion with redundant-copies=2
    String regionName = getTestMethodName() + "_PR";
    vm1.invoke(() -> createPartitionedRegion(regionName, regionShortcut, aeqId, 2));
    vm2.invoke(() -> createPartitionedRegion(regionName, regionShortcut, aeqId, 2));
    vm3.invoke(() -> createPartitionedRegion(regionName, regionShortcut, aeqId, 2));

    // Create primary bucket in member 1, secondary buckets in members 2 and 3
    Object key = "0";
    vm1.invoke(() -> createBucket(regionName, key));
    vm2.invoke(() -> createBucket(regionName, key));
    vm3.invoke(() -> createBucket(regionName, key));
    vm1.invoke(() -> assertPrimaryBucket(regionName, key));

    // Fake a replication event in member 2
    InternalDistributedMember idmMember1 = vm1.invoke(this::getMember);
    VersionSource vsMember1 = vm1.invoke(() -> getVersionMember(regionName));
    vm2.invoke(() -> doFakeUpdate(idmMember1, vsMember1, regionName, key));

    // Verify only member 2's queue contains an event
    vm1.invoke(() -> checkAsyncEventQueueSize(aeqId, 0, true));
    vm2.invoke(() -> checkAsyncEventQueueSize(aeqId, 1, true));
    vm3.invoke(() -> checkAsyncEventQueueSize(aeqId, 0, true));

    // Crash member 1
    DistributedTestUtils.crashDistributedSystem(vm1);

    // Wait for member 3's queue to contain 1 event (which means it has been synchronized)
    vm3.invoke(() -> waitForAsyncEventQueueSize(aeqId, 1, true));

    // Start processing events (mainly to avoid an InterruptedException suspect string)
    vm2.invoke(() -> startProcessingAsyncEvents(aeqId));
    vm3.invoke(() -> startProcessingAsyncEvents(aeqId));
  }

  private void createPartitionedRegion(String regionName, RegionShortcut regionShortcut,
      String aeqId, int redundantCopies) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies);

    cache.createRegionFactory(regionShortcut).addAsyncEventQueueId(aeqId)
        .setPartitionAttributes(paf.create()).create(regionName);
  }

  private void createBucket(String regionName, Object key) throws Exception {
    PartitionedRegion pr = (PartitionedRegion) cache.getRegion(regionName);
    int bucketId = PartitionedRegionHelper.getHashKey(pr, null, key, null, null);
    pr.getRedundancyProvider().createBackupBucketOnMember(bucketId, getMember(), false, false, null,
        true);
  }

  private void assertPrimaryBucket(String regionName, Object key) throws Exception {
    PartitionedRegion pr = (PartitionedRegion) cache.getRegion(regionName);
    int bucketId = PartitionedRegionHelper.getHashKey(pr, null, key, null, null);
    assertThat(pr.getRegionAdvisor().isPrimaryForBucket(bucketId)).isTrue();
  }

  private InternalDistributedMember getMember() {
    return (InternalDistributedMember) cache.getDistributedSystem().getDistributedMember();
  }

  private VersionSource getVersionMember(String regionName) {
    PartitionedRegion pr = (PartitionedRegion) cache.getRegion(regionName);
    return pr.getVersionMember();
  }

  private void doFakeUpdate(InternalDistributedMember fromMember, VersionSource versionSource,
      String regionName, Object key) {
    // Get the BucketRegion for the regionName and key
    PartitionedRegion pr = (PartitionedRegion) cache.getRegion(regionName);
    BucketRegion br = pr.getBucketRegion(key);

    // Create VersionTag
    long timestamp = System.currentTimeMillis();
    VersionTag tag = new VMVersionTag();
    tag.setMemberID(versionSource);
    tag.setRegionVersion(1);
    tag.setEntryVersion(1);
    tag.setVersionTimeStamp(timestamp);
    tag.setIsRemoteForTesting();

    EntryEventImpl event =
        EntryEventImpl.create(br, Operation.CREATE, key, true, fromMember, true, false);
    event.setNewValue(new VMCachedDeserializable("0", 0));
    event.setTailKey(161l);
    event.setVersionTag(tag);
    event.setEventId(new EventID(cache.getDistributedSystem()));

    // Put event into region
    br.getRegionMap().basicPut(event, timestamp, true, false, null, false, false);
  }

  private void startProcessingAsyncEvents(String aeqId) {
    // Get the async event listener
    WaitingAsyncEventListener listener = getWaitingAsyncEventListener(aeqId);

    // Start processing waiting events
    listener.startProcessingEvents();
  }

  private WaitingAsyncEventListener getWaitingAsyncEventListener(String aeqId) {
    // Get the async event queue
    AsyncEventQueue aeq = cache.getAsyncEventQueue(aeqId);
    assertThat(aeq).isNotNull();

    // Get and return the async event listener
    AsyncEventListener aeqListener = aeq.getAsyncEventListener();
    assertThat(aeqListener).isInstanceOf(WaitingAsyncEventListener.class);
    return (WaitingAsyncEventListener) aeqListener;
  }
}
