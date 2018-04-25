/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock;
import org.apache.geode.test.junit.categories.IntegrationTest;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.script.*", "javax.management.*", "org.springframework.shell.event.*",
    "org.springframework.shell.core.*", "*.IntegrationTest"})
@PrepareForTest({CacheClientNotifier.class})
@Category(IntegrationTest.class)
public class HARegionQueueIntegrationTest {

  private Cache cache;

  private Region dataRegion;

  private CacheClientNotifier ccn;

  private InternalDistributedMember member;

  private static final int NUM_QUEUES = 100;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    cache = createCache();
    dataRegion = createDataRegion();
    ccn = createCacheClientNotifier();
    member = createMember();
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  private Cache createCache() {
    return new CacheFactory().set(MCAST_PORT, "0").create();
  }

  private Region createDataRegion() {
    return cache.createRegionFactory(RegionShortcut.REPLICATE).create("data");
  }

  private CacheClientNotifier createCacheClientNotifier() {
    // Create a mock CacheClientNotifier
    CacheClientNotifier ccn = mock(CacheClientNotifier.class);
    PowerMockito.mockStatic(CacheClientNotifier.class, Mockito.CALLS_REAL_METHODS);
    PowerMockito.when(CacheClientNotifier.getInstance()).thenReturn(ccn);
    return ccn;
  }

  private InternalDistributedMember createMember() {
    // Create an InternalDistributedMember
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getVersionObject()).thenReturn(Version.CURRENT);
    return member;
  }

  @Test
  public void verifySequentialUpdateHAEventWrapperWithMap() throws Exception {
    // Create a HAContainerMap to be used by the CacheClientNotifier
    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSequentially(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  @Test
  public void verifySimultaneousUpdateHAEventWrapperWithMap() throws Exception {
    // Create a HAContainerMap to be used by the CacheClientNotifier
    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSimultaneously(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  @Test
  public void verifySequentialUpdateHAEventWrapperWithRegion() throws Exception {
    // Create a HAContainerRegion to be used by the CacheClientNotifier
    HAContainerWrapper haContainerWrapper = createHAContainerRegion();
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSequentially(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  @Test
  public void verifySimultaneousUpdateHAEventWrapperWithRegion() throws Exception {
    // Create a HAContainerRegion to be used by the CacheClientNotifier
    HAContainerWrapper haContainerWrapper = createHAContainerRegion();
    when(ccn.getHaContainer()).thenReturn(haContainerWrapper);

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSimultaneously(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  private HAContainerRegion createHAContainerRegion() throws Exception {
    // Create a Region to be used by the HAContainerRegion
    Region haContainerRegionRegion = createHAContainerRegionRegion();

    // Create an HAContainerRegion
    HAContainerRegion haContainerRegion = new HAContainerRegion(haContainerRegionRegion);

    return haContainerRegion;
  }

  private Region createHAContainerRegionRegion() throws Exception {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.LOCAL);
    factory.setDiskStoreName(null);
    factory.setDiskSynchronous(true);
    factory.setDataPolicy(DataPolicy.NORMAL);
    factory.setStatisticsEnabled(true);
    factory.setEvictionAttributes(
        EvictionAttributes.createLIFOEntryAttributes(1000, EvictionAction.OVERFLOW_TO_DISK));
    Region region = ((GemFireCacheImpl) cache).createVMRegion(
        CacheServerImpl.generateNameForClientMsgsRegion(0), factory.create(),
        new InternalRegionArguments().setDestroyLockFlag(true).setRecreateFlag(false)
            .setSnapshotInputStream(null).setImageTarget(null).setIsUsedForMetaRegion(true));
    return region;
  }

  private HARegionQueue createHARegionQueue(Map haContainer, int index) throws Exception {
    return new HARegionQueue("haRegion+" + index, mock(HARegion.class), (InternalCache) cache,
        haContainer, null, (byte) 1, true, mock(HARegionQueueStats.class),
        mock(StoppableReentrantReadWriteLock.class), mock(StoppableReentrantReadWriteLock.class),
        mock(CancelCriterion.class), false);
  }

  private CachedDeserializable createCachedDeserializable(HAContainerWrapper haContainerWrapper)
      throws Exception {
    // Create ClientUpdateMessage and HAEventWrapper
    ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), new EventID(cache.getDistributedSystem()));
    HAEventWrapper wrapper = new HAEventWrapper(message);
    wrapper.setHAContainer(haContainerWrapper);

    // Create a CachedDeserializable
    // Note: The haContainerRegion must contain the wrapper and message to serialize it
    haContainerWrapper.putIfAbsent(wrapper, message);
    byte[] wrapperBytes = BlobHelper.serializeToBlob(wrapper);
    CachedDeserializable cd = new VMCachedDeserializable(wrapperBytes);
    haContainerWrapper.remove(wrapper);
    assertThat(haContainerWrapper.size()).isEqualTo(0);
    return cd;
  }

  private void createAndUpdateHARegionQueuesSequentially(HAContainerWrapper haContainerWrapper,
      CachedDeserializable cd, int numQueues) throws Exception {
    // Create some HARegionQueues
    for (int i = 0; i < numQueues; i++) {
      HARegionQueue haRegionQueue = createHARegionQueue(haContainerWrapper, i);
      haRegionQueue.updateHAEventWrapper(member, cd, "haRegion");
    }
  }

  private void createAndUpdateHARegionQueuesSimultaneously(HAContainerWrapper haContainerWrapper,
      CachedDeserializable cd, int numQueues) throws Exception {
    // Create some HARegionQueues
    HARegionQueue[] haRegionQueues = new HARegionQueue[numQueues];
    for (int i = 0; i < numQueues; i++) {
      haRegionQueues[i] = createHARegionQueue(haContainerWrapper, i);
    }

    // Create threads to simultaneously update the HAEventWrapper
    int j = 0;
    Thread[] threads = new Thread[numQueues];
    for (HARegionQueue haRegionQueue : haRegionQueues) {
      threads[j] = new Thread(() -> {
        haRegionQueue.updateHAEventWrapper(member, cd, "haRegion");
      });
      j++;
    }

    // Start the threads
    for (int i = 0; i < numQueues; i++) {
      threads[i].start();
    }

    // Wait for the threads to complete
    for (int i = 0; i < numQueues; i++) {
      threads[i].join();
    }
  }

  private void verifyHAContainerWrapper(HAContainerWrapper haContainerWrapper,
      CachedDeserializable cd, int numQueues) {
    // Verify HAContainerRegion size
    assertThat(haContainerWrapper.size()).isEqualTo(1);

    // Verify the refCount is correct
    HAEventWrapper wrapperInContainer =
        (HAEventWrapper) haContainerWrapper.getKey(cd.getDeserializedForReading());
    assertThat(wrapperInContainer.getReferenceCount()).isEqualTo(numQueues);
  }
}
