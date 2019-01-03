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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.awaitility.core.ConditionTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import util.TestException;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.SystemTimer;
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
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.ConnectionListener;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock;

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
    HAContainerWrapper haContainerWrapper = (HAContainerWrapper) ccn.getHaContainer();
  }

  @After
  public void tearDown() throws Exception {
    ccn.shutdown(0);
    cache.close();
  }

  private Cache createCache() {
    return new CacheFactory().set(MCAST_PORT, "0").create();
  }

  private Region createDataRegion() {
    return cache.createRegionFactory(RegionShortcut.REPLICATE).create("data");
  }

  private InternalCache createMockInternalCache() {
    InternalCache mockInternalCache = mock(InternalCache.class);
    doReturn(mock(SystemTimer.class)).when(mockInternalCache).getCCPTimer();
    doReturn(mock(CancelCriterion.class)).when(mockInternalCache).getCancelCriterion();

    InternalDistributedSystem mockInteralDistributedSystem = createMockInternalDistributedSystem();
    doReturn(mockInteralDistributedSystem).when(mockInternalCache).getInternalDistributedSystem();
    doReturn(mockInteralDistributedSystem).when(mockInternalCache).getDistributedSystem();

    return mockInternalCache;
  }

  private InternalDistributedSystem createMockInternalDistributedSystem() {
    InternalDistributedSystem mockInternalDistributedSystem =
        mock(InternalDistributedSystem.class);
    DistributionManager mockDistributionManager = mock(DistributionManager.class);

    doReturn(mock(InternalDistributedMember.class)).when(mockInternalDistributedSystem)
        .getDistributedMember();
    doReturn(mock(Statistics.class)).when(mockInternalDistributedSystem)
        .createAtomicStatistics(any(StatisticsType.class), any(String.class));
    doReturn(mock(DistributionConfig.class)).when(mockDistributionManager).getConfig();
    doReturn(mockDistributionManager).when(mockInternalDistributedSystem).getDistributionManager();
    doReturn(mock(DSClock.class)).when(mockInternalDistributedSystem).getClock();

    return mockInternalDistributedSystem;
  }

  private CacheClientNotifier createCacheClientNotifier() {
    CacheClientNotifier ccn =
        CacheClientNotifier.getInstance((InternalCache) cache, mock(CacheServerStats.class),
            100000, 100000, mock(ConnectionListener.class), null, false);
    return ccn;
  }

  private InternalDistributedMember createMember() {
    // Create an InternalDistributedMember
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getVersionObject()).thenReturn(Version.CURRENT);
    return member;
  }

  @Test
  public void verifyEndGiiQueueingEmptiesQueueAndHAContainer() throws Exception {
    HAContainerWrapper haContainerWrapper = (HAContainerWrapper) ccn.getHaContainer();

    // create message and HAEventWrapper
    ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), new EventID(cache.getDistributedSystem()));
    HAEventWrapper wrapper = new HAEventWrapper(message);
    wrapper.setHAContainer(haContainerWrapper);
    wrapper.incrementPutInProgressCounter();

    // Create and update HARegionQueues forcing one queue to startGiiQueueing
    int numQueues = 10;
    HARegionQueue targetQueue = createAndUpdateHARegionQueuesWithGiiQueueing(haContainerWrapper,
        wrapper, message, numQueues);

    // Verify HAContainerWrapper (1) and refCount (numQueues(10))
    assertEquals(1, haContainerWrapper.size());

    HAEventWrapper wrapperInContainer = (HAEventWrapper) haContainerWrapper.getKey(wrapper);
    assertEquals(numQueues - 1, wrapperInContainer.getReferenceCount());
    assertTrue(wrapperInContainer.getPutInProgress());

    // Verify that the HAEventWrapper in the giiQueue now has msg != null
    // We don't null this out while putInProgress > 0 (true)
    Queue giiQueue = targetQueue.getGiiQueue();
    assertEquals(1, giiQueue.size());

    // Simulate that we have iterated through all interested proxies
    // and are now decrementing the PutInProgressCounter
    wrapperInContainer.decrementPutInProgressCounter();

    // Simulate that other queues have processed this event, then
    // peek and process the event off the giiQueue
    for (int i = 0; i < numQueues - 1; ++i) {
      targetQueue.decAndRemoveFromHAContainer(wrapper);
    }

    HAEventWrapper giiQueueEntry = (HAEventWrapper) giiQueue.peek();
    assertNotNull(giiQueueEntry);
    assertNotNull(giiQueueEntry.getClientUpdateMessage());

    // endGiiQueueing and verify queue and HAContainer are empty
    targetQueue.endGiiQueueing();
    assertEquals(0, giiQueue.size());

    Assert.assertEquals("Expected HAContainer to be empty", 0, haContainerWrapper.size());
  }

  @Test
  public void verifySequentialUpdateHAEventWrapperWithMap() throws Exception {
    HAContainerWrapper haContainerWrapper = (HAContainerWrapper) ccn.getHaContainer();

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSequentially(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  @Test
  public void verifySimultaneousUpdateHAEventWrapperWithMap() throws Exception {
    HAContainerWrapper haContainerWrapper = (HAContainerWrapper) ccn.getHaContainer();

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSimultaneously(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  @Test
  public void verifySequentialUpdateHAEventWrapperWithRegion() throws Exception {
    HAContainerWrapper haContainerWrapper = (HAContainerWrapper) ccn.getHaContainer();

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSequentially(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  @Test
  public void verifySimultaneousUpdateHAEventWrapperWithRegion() throws Exception {
    HAContainerWrapper haContainerWrapper = (HAContainerWrapper) ccn.getHaContainer();

    // Create a CachedDeserializable
    CachedDeserializable cd = createCachedDeserializable(haContainerWrapper);

    // Create and update HARegionQueues
    createAndUpdateHARegionQueuesSimultaneously(haContainerWrapper, cd, NUM_QUEUES);

    // Verify HAContainerWrapper
    verifyHAContainerWrapper(haContainerWrapper, cd, NUM_QUEUES);
  }

  @Test
  public void verifySimultaneousPutHAEventWrapperWithRegion() throws Exception {
    HAContainerWrapper haContainerWrapper = createHAContainerRegion();

    final int numQueues = 30;
    final int numOperations = 1000;

    Set<HAEventWrapper> haEventWrappersToValidate =
        createAndPutHARegionQueuesSimulataneously(haContainerWrapper, numQueues, numOperations);

    assertEquals(numOperations, haContainerWrapper.size());

    for (HAEventWrapper haEventWrapperToValidate : haEventWrappersToValidate) {
      HAEventWrapper wrapperInContainer =
          (HAEventWrapper) haContainerWrapper.getKey(haEventWrapperToValidate);
      assertEquals(numQueues, wrapperInContainer.getReferenceCount());
    }
  }

  @Test
  public void verifySequentialPutHAEventWrapperWithRegion() throws Exception {
    HAContainerWrapper haContainerWrapper = createHAContainerRegion();

    ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), new EventID(new byte[] {1}, 1, 2));
    HAEventWrapper haEventWrapper = new HAEventWrapper(message);
    haEventWrapper.setHAContainer(haContainerWrapper);

    final int numQueues = 10;

    createAndPutHARegionQueuesSequentially(haContainerWrapper, haEventWrapper, numQueues);

    assertEquals(1, haContainerWrapper.size());

    HAEventWrapper wrapperInContainer =
        (HAEventWrapper) haContainerWrapper.getKey(haEventWrapper);
    assertEquals(numQueues, wrapperInContainer.getReferenceCount());
  }

  @Test
  public void verifySimultaneousPutHAEventWrapperWithMap() throws Exception {
    HAContainerWrapper haContainerWrapper = (HAContainerWrapper) ccn.getHaContainer();

    final int numQueues = 30;
    final int numOperations = 1000;

    Set<HAEventWrapper> haEventWrappersToValidate =
        createAndPutHARegionQueuesSimulataneously(haContainerWrapper, numQueues, numOperations);

    assertEquals(numOperations, haContainerWrapper.size());

    for (HAEventWrapper haEventWrapperToValidate : haEventWrappersToValidate) {
      HAEventWrapper wrapperInContainer =
          (HAEventWrapper) haContainerWrapper.getKey(haEventWrapperToValidate);
      assertEquals(numQueues, wrapperInContainer.getReferenceCount());
    }
  }

  @Test
  public void verifySequentialPutHAEventWrapperWithMap() throws Exception {
    HAContainerWrapper haContainerWrapper = (HAContainerWrapper) ccn.getHaContainer();

    ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), new EventID(new byte[] {1}, 1, 2));
    HAEventWrapper haEventWrapper = new HAEventWrapper(message);
    haEventWrapper.setHAContainer(haContainerWrapper);

    final int numQueues = 10;
    createAndPutHARegionQueuesSequentially(haContainerWrapper, haEventWrapper, numQueues);

    assertEquals(1, haContainerWrapper.size());

    HAEventWrapper wrapperInContainer =
        (HAEventWrapper) haContainerWrapper.getKey(haEventWrapper);
    assertEquals(numQueues, wrapperInContainer.getReferenceCount());
  }

  @Test
  public void queueRemovalAndDispatchingConcurrently() throws Exception {
    HAContainerWrapper haContainerWrapper = (HAContainerWrapper) ccn.getHaContainer();

    List<HARegionQueue> regionQueues = new ArrayList<>();

    for (int i = 0; i < 2; ++i) {
      HARegion haRegion = createMockHARegion();

      regionQueues.add(createHARegionQueue(haContainerWrapper, i, haRegion, false));
    }

    ExecutorService service = Executors.newFixedThreadPool(2);

    List<Callable<Object>> callables = new ArrayList<>();

    for (int i = 0; i < 10000; ++i) {
      callables.clear();

      EventID eventID = new EventID(new byte[] {1}, 1, i);

      ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
          (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
          new ClientProxyMembershipID(), eventID);

      HAEventWrapper wrapper = new HAEventWrapper(message);
      wrapper.setHAContainer(haContainerWrapper);
      wrapper.incrementPutInProgressCounter();

      for (HARegionQueue queue : regionQueues) {
        queue.put(wrapper);
      }

      wrapper.decrementPutInProgressCounter();

      for (HARegionQueue queue : regionQueues) {
        callables.add(Executors.callable(() -> {
          try {
            queue.peek();
            queue.remove();
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }));

        callables.add(Executors.callable(() -> {
          try {
            queue.removeDispatchedEvents(eventID);
          } catch (Exception ex) {
            throw new RuntimeException(ex);
          }
        }));
      }

      // invokeAll() will wait until our two callables have completed
      List<Future<Object>> futures = service.invokeAll(callables, 10, TimeUnit.SECONDS);

      for (Future<Object> future : futures) {
        try {
          future.get();
        } catch (Exception ex) {
          throw new TestException(
              "Exception thrown while executing regionQueue methods concurrently on iteration: "
                  + i,
              ex);
        }
      }
    }
  }

  @Test
  public void verifyPutEntryConditionallyInHAContainerNoOverwrite() throws Exception {
    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());

    // create message and HAEventWrapper
    EventID eventID = new EventID(cache.getDistributedSystem());
    ClientUpdateMessage oldMessage = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), eventID);
    HAEventWrapper originalWrapperInstance = new HAEventWrapper(oldMessage);
    originalWrapperInstance.incrementPutInProgressCounter();
    originalWrapperInstance.setHAContainer(haContainerWrapper);

    HARegionQueue haRegionQueue = createHARegionQueue(haContainerWrapper, 0);

    haRegionQueue.putEventInHARegion(originalWrapperInstance, 1L);

    // Simulate a QRM for this event
    haRegionQueue.region.destroy(1L);
    haRegionQueue.decAndRemoveFromHAContainer(originalWrapperInstance);

    ClientUpdateMessage newMessage = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), eventID);
    HAEventWrapper newWrapperInstance = new HAEventWrapper(newMessage);
    newWrapperInstance.incrementPutInProgressCounter();
    newWrapperInstance.setHAContainer(haContainerWrapper);

    haRegionQueue.putEventInHARegion(newWrapperInstance, 1L);

    // Add the original wrapper back in, and verify that it does not overwrite the new one
    // and that it increments the ref count on the container key.
    haRegionQueue.putEventInHARegion(originalWrapperInstance, 1L);

    Assert.assertEquals("Original message overwrote new message in container",
        haContainerWrapper.get(originalWrapperInstance),
        newWrapperInstance.getClientUpdateMessage());
    Assert.assertEquals("Reference count was not the expected value", 2,
        newWrapperInstance.getReferenceCount());
    Assert.assertEquals("Container size was not the expected value", haContainerWrapper.size(), 1);
  }

  @Test
  public void removeDispatchedEventsViaQRMAndDestroyQueueSimultaneouslySingleDecrement()
      throws Exception {
    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());

    HARegion haRegion = createMockHARegion();
    HARegionQueue haRegionQueue = createHARegionQueue(haContainerWrapper, 0, haRegion, false);

    EventID eventID = new EventID(cache.getDistributedSystem());
    ClientUpdateMessage clientUpdateMessage =
        new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
            (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
            new ClientProxyMembershipID(), eventID);
    HAEventWrapper haEventWrapper = new HAEventWrapper(clientUpdateMessage);
    haEventWrapper.incrementPutInProgressCounter();
    haEventWrapper.setHAContainer(haContainerWrapper);

    haRegionQueue.put(haEventWrapper);

    ExecutorService service = Executors.newFixedThreadPool(2);

    List<Callable<Object>> callables = new ArrayList<>();

    // In one thread, simulate processing a queue removal message
    // by removing the dispatched event
    callables.add(Executors.callable(() -> {
      try {
        haRegionQueue.removeDispatchedEvents(eventID);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }));

    // In another thread, simulate that the region is being destroyed, for instance
    // when a SocketTimeoutException is thrown and we are cleaning up
    callables.add(Executors.callable(() -> {
      try {
        haRegionQueue.destroy();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }));

    List<Future<Object>> futures = service.invokeAll(callables, 10, TimeUnit.SECONDS);

    for (Future<Object> future : futures) {
      try {
        future.get();
      } catch (Exception ex) {
        throw new TestException(
            "Exception thrown while executing queue removal and destroy region queue logic concurrently.",
            ex);
      }
    }

    try {
      await().until(() -> haEventWrapper.getReferenceCount() == 0);
    } catch (ConditionTimeoutException conditionTimeoutException) {
      throw new TestException(
          "Expected HAEventWrapper reference count to be decremented to 0 by either the queue removal or destroy queue logic, but the actual reference count was "
              + haEventWrapper.getReferenceCount());
    }
  }

  @Test
  public void removeDispatchedEventsViaMessageDispatcherAndDestroyQueueSimultaneouslySingleDecrement()
      throws Exception {
    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());

    HARegion haRegion = createMockHARegion();
    HARegionQueue haRegionQueue = createHARegionQueue(haContainerWrapper, 0, haRegion, false);

    EventID eventID = new EventID(cache.getDistributedSystem());
    ClientUpdateMessage clientUpdateMessage =
        new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
            (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
            new ClientProxyMembershipID(), eventID);
    HAEventWrapper haEventWrapper = new HAEventWrapper(clientUpdateMessage);
    haEventWrapper.incrementPutInProgressCounter();
    haEventWrapper.setHAContainer(haContainerWrapper);

    haRegionQueue.put(haEventWrapper);

    ExecutorService service = Executors.newFixedThreadPool(2);

    List<Callable<Object>> callables = new ArrayList<>();

    // In one thread, simulate processing a queue removal message
    // by removing the dispatched event
    callables.add(Executors.callable(() -> {
      try {
        // Simulate dispatching a message by peeking and removing the HAEventWrapper
        haRegionQueue.peek();
        haRegionQueue.remove();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }));

    // In another thread, simulate that the region is being destroyed, for instance
    // when a SocketTimeoutException is thrown and we are cleaning up
    callables.add(Executors.callable(() -> {
      try {
        haRegionQueue.destroy();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }));

    List<Future<Object>> futures = service.invokeAll(callables, 10, TimeUnit.SECONDS);

    for (Future<Object> future : futures) {
      try {
        future.get();
      } catch (Exception ex) {
        throw new TestException(
            "Exception thrown while executing message dispatching and destroy region queue logic concurrently.",
            ex);
      }
    }

    try {
      await().until(() -> haEventWrapper.getReferenceCount() == 0);
    } catch (ConditionTimeoutException conditionTimeoutException) {
      throw new TestException(
          "Expected HAEventWrapper reference count to be decremented to 0 by either the message dispatcher or destroy queue logic, but the actual reference count was "
              + haEventWrapper.getReferenceCount());
    }
  }

  private HARegion createMockHARegion() {
    HARegion haRegion = mock(HARegion.class);
    when(haRegion.getGemFireCache()).thenReturn((InternalCache) cache);

    ConcurrentHashMap<Object, Object> mockRegion = new ConcurrentHashMap<>();

    when(haRegion.put(Mockito.any(Object.class), Mockito.any(Object.class))).then(answer -> {
      Object existingValue = mockRegion.put(answer.getArgument(0), answer.getArgument(1));
      return existingValue;
    });

    when(haRegion.get(Mockito.any(Object.class))).then(answer -> {
      return mockRegion.get(answer.getArgument(0));
    });

    doAnswer(answer -> {
      mockRegion.remove(answer.getArgument(0));
      return null;
    }).when(haRegion).localDestroy(Mockito.any(Object.class));
    return haRegion;
  }

  private HAContainerRegion createHAContainerRegion() throws Exception {
    Region haContainerRegionRegion = createHAContainerRegionRegion();

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

  private HARegionQueue createHARegionQueue(Map haContainer, int index, HARegion haRegion,
      boolean puttingGIIDataInQueue) throws Exception {
    StoppableReentrantReadWriteLock giiLock = mock(StoppableReentrantReadWriteLock.class);
    doReturn(mock(StoppableReentrantReadWriteLock.StoppableWriteLock.class)).when(giiLock)
        .writeLock();
    doReturn(mock(StoppableReentrantReadWriteLock.StoppableReadLock.class)).when(giiLock)
        .readLock();

    StoppableReentrantReadWriteLock rwLock =
        new StoppableReentrantReadWriteLock(cache.getCancelCriterion());

    return new HARegionQueue("haRegion+" + index, haRegion, (InternalCache) cache, haContainer,
        null, (byte) 1, true, mock(HARegionQueueStats.class), giiLock, rwLock,
        mock(CancelCriterion.class), puttingGIIDataInQueue);
  }

  private HARegionQueue createHARegionQueue(Map haContainer, int index) throws Exception {
    HARegion haRegion = mock(HARegion.class);
    when(haRegion.getGemFireCache()).thenReturn((InternalCache) cache);

    return createHARegionQueue(haContainer, index, haRegion, false);
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

  private HARegionQueue createAndUpdateHARegionQueuesWithGiiQueueing(
      HAContainerWrapper haContainerWrapper, HAEventWrapper wrapper, ClientUpdateMessage message,
      int numQueues) throws Exception {

    HARegionQueue targetQueue = null;
    int startGiiQueueingIndex = numQueues / 2;

    // create HARegionQueues and startGiiQueuing on a region about half way through
    for (int i = 0; i < numQueues; i++) {
      HARegionQueue haRegionQueue = null;

      // start GII Queueing (targetRegionQueue)
      if (i == startGiiQueueingIndex) {
        HARegion haRegion = mock(HARegion.class);

        final HARegionQueue giiHaRegionQueue =
            createHARegionQueue(haContainerWrapper, i, haRegion, false);;
        giiHaRegionQueue.startGiiQueueing();
        targetQueue = giiHaRegionQueue;

        when(haRegion.put(Mockito.any(Object.class), Mockito.any(HAEventWrapper.class)))
            .then(answer -> {
              // Simulate that either a QRM or message dispatch has occurred immediately after the
              // put.
              // We want to ensure that the event is removed from the HAContainer if it is drained
              // from the giiQueue
              // and the ref count has dropped to 0.
              HAEventWrapper haContainerKey = answer.getArgument(1);
              giiHaRegionQueue.decAndRemoveFromHAContainer(haContainerKey);
              return null;
            });

        when(haRegion.getGemFireCache()).thenReturn((InternalCache) cache);

        haRegionQueue = giiHaRegionQueue;
      } else {
        haRegionQueue = createHARegionQueue(haContainerWrapper, i);
      }

      haRegionQueue.put(wrapper);
    }

    return targetQueue;
  }

  private Set<HAEventWrapper> createAndPutHARegionQueuesSimulataneously(
      HAContainerWrapper haContainerWrapper, int numQueues, int numOperations) throws Exception {
    ConcurrentLinkedQueue<HARegionQueue> queues = new ConcurrentLinkedQueue<>();
    final ConcurrentHashSet<HAEventWrapper> testValidationWrapperSet = new ConcurrentHashSet<>();
    final AtomicInteger count = new AtomicInteger();

    // create HARegionQueuesv
    for (int i = 0; i < numQueues; i++) {
      queues.add(createHARegionQueue(haContainerWrapper, i));
    }

    for (int i = 0; i < numOperations; i++) {
      count.set(i);

      ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_CREATE,
          (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
          new ClientProxyMembershipID(), new EventID(new byte[] {1}, 1, count.get()));

      queues.parallelStream().forEach(haRegionQueue -> {
        try {
          // In production (CacheClientNotifier.singletonRouteClientMessage), each queue has its
          // own HAEventWrapper object even though they hold the same ClientUpdateMessage,
          // so we create an object for each queue in here
          HAEventWrapper haEventWrapper = new HAEventWrapper(message);

          testValidationWrapperSet.add(haEventWrapper);

          haRegionQueue.put(haEventWrapper);
        } catch (InterruptedException iex) {
          throw new RuntimeException(iex);
        }
      });
    }

    return testValidationWrapperSet;
  }

  private void createAndPutHARegionQueuesSequentially(HAContainerWrapper haContainerWrapper,
      HAEventWrapper haEventWrapper, int numQueues) throws Exception {
    ArrayList<HARegionQueue> queues = new ArrayList<>();

    // create HARegionQueues
    for (int i = 0; i < numQueues; i++) {
      queues.add(createHARegionQueue(haContainerWrapper, i));
    }

    haEventWrapper.incrementPutInProgressCounter();

    for (HARegionQueue queue : queues) {
      queue.put(haEventWrapper);
    }

    haEventWrapper.decrementPutInProgressCounter();
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
