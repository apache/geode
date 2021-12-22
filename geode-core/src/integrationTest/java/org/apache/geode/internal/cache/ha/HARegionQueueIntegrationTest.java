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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientRegistrationEventQueueManager;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.ConnectionListener;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock.StoppableReadLock;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock.StoppableWriteLock;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class HARegionQueueIntegrationTest {

  private static final int NUM_QUEUES = 100;
  private static final EvictionAttributes OVERFLOW_TO_DISK =
      EvictionAttributes.createLIFOEntryAttributes(1000, EvictionAction.OVERFLOW_TO_DISK);

  private InternalCache cache;
  private Region dataRegion;
  private CacheClientNotifier ccn;
  private InternalDistributedMember member;

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();
  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.LENIENT);

  @Before
  public void setUp() throws Exception {
    cache = createCache();
    dataRegion = createDataRegion();
    ccn = createCacheClientNotifier();
    member = createMember();
  }

  @After
  public void tearDown() throws Exception {
    ccn.shutdown(0);
    cache.close();
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
    wrapper.incrementPutInProgressCounter("test");

    // Create and update HARegionQueues forcing one queue to startGiiQueueing
    int numQueues = 10;
    HARegionQueue targetQueue = createAndUpdateHARegionQueuesWithGiiQueueing(haContainerWrapper,
        wrapper, numQueues);

    // Verify HAContainerWrapper (1) and refCount (numQueues(10))
    assertThat(haContainerWrapper).hasSize(1);

    HAEventWrapper wrapperInContainer = (HAEventWrapper) haContainerWrapper.getKey(wrapper);
    assertThat(wrapperInContainer.getReferenceCount()).isEqualTo(numQueues - 1);
    assertThat(wrapperInContainer.getPutInProgress()).isTrue();

    // Verify that the HAEventWrapper in the giiQueue now has msg != null
    // We don't null this out while putInProgress > 0 (true)
    Queue giiQueue = targetQueue.getGiiQueue();
    assertThat(giiQueue).hasSize(1);

    // Simulate that we have iterated through all interested proxies
    // and are now decrementing the PutInProgressCounter
    wrapperInContainer.decrementPutInProgressCounter();

    // Simulate that other queues have processed this event, then
    // peek and process the event off the giiQueue
    for (int i = 0; i < numQueues - 1; ++i) {
      targetQueue.decAndRemoveFromHAContainer(wrapper);
    }

    HAEventWrapper giiQueueEntry = (HAEventWrapper) giiQueue.peek();
    assertThat(giiQueueEntry).isNotNull();
    assertThat(giiQueueEntry.getClientUpdateMessage()).isNotNull();

    // endGiiQueueing and verify queue and HAContainer are empty
    targetQueue.endGiiQueueing();

    assertThat(giiQueue).isEmpty();
    assertThat(haContainerWrapper).isEmpty();
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
    int numQueues = 30;
    int numOperations = 1000;

    Set<HAEventWrapper> haEventWrappersToValidate =
        createAndPutHARegionQueuesSimultaneously(haContainerWrapper, numQueues, numOperations);

    assertThat(haContainerWrapper).hasSize(numOperations);

    for (HAEventWrapper haEventWrapperToValidate : haEventWrappersToValidate) {
      HAEventWrapper wrapperInContainer =
          (HAEventWrapper) haContainerWrapper.getKey(haEventWrapperToValidate);
      assertThat(wrapperInContainer.getReferenceCount()).isEqualTo(numQueues);
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

    int numQueues = 10;

    createAndPutHARegionQueuesSequentially(haContainerWrapper, haEventWrapper, numQueues);

    assertThat(haContainerWrapper).hasSize(1);

    HAEventWrapper wrapperInContainer =
        (HAEventWrapper) haContainerWrapper.getKey(haEventWrapper);
    assertThat(wrapperInContainer.getReferenceCount()).isEqualTo(numQueues);
  }

  @Test
  public void verifySimultaneousPutHAEventWrapperWithMap() throws Exception {
    HAContainerWrapper haContainerWrapper = (HAContainerWrapper) ccn.getHaContainer();
    int numQueues = 30;
    int numOperations = 1000;

    Set<HAEventWrapper> haEventWrappersToValidate =
        createAndPutHARegionQueuesSimultaneously(haContainerWrapper, numQueues, numOperations);

    assertThat(haContainerWrapper).hasSize(numOperations);

    for (HAEventWrapper haEventWrapperToValidate : haEventWrappersToValidate) {
      HAEventWrapper wrapperInContainer =
          (HAEventWrapper) haContainerWrapper.getKey(haEventWrapperToValidate);
      assertThat(wrapperInContainer.getReferenceCount()).isEqualTo(numQueues);
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

    int numQueues = 10;

    createAndPutHARegionQueuesSequentially(haContainerWrapper, haEventWrapper, numQueues);

    assertThat(haContainerWrapper).hasSize(1);

    HAEventWrapper wrapperInContainer =
        (HAEventWrapper) haContainerWrapper.getKey(haEventWrapper);
    assertThat(wrapperInContainer.getReferenceCount()).isEqualTo(numQueues);
  }

  @Test
  public void queueRemovalAndDispatchingConcurrently() throws Exception {
    HAContainerWrapper haContainerWrapper = (HAContainerWrapper) ccn.getHaContainer();

    List<HARegionQueue> regionQueues = new ArrayList<>();
    for (int i = 0; i < 2; ++i) {
      HARegion haRegion = createMockHARegion();

      regionQueues.add(createHARegionQueue(haContainerWrapper, i, haRegion, false));
    }

    for (int i = 0; i < 10000; ++i) {
      EventID eventID = new EventID(new byte[] {1}, 1, i);

      ClientUpdateMessage message = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
          (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
          new ClientProxyMembershipID(), eventID);

      HAEventWrapper wrapper = new HAEventWrapper(message);
      wrapper.setHAContainer(haContainerWrapper);
      wrapper.incrementPutInProgressCounter("test");

      for (HARegionQueue queue : regionQueues) {
        queue.put(wrapper);
      }

      wrapper.decrementPutInProgressCounter();

      List<Future<Void>> futures = new ArrayList<>();
      for (HARegionQueue queue : regionQueues) {
        futures.add(executorServiceRule.submit(() -> {
          queue.peek();
          queue.remove();
        }));

        futures.add(executorServiceRule.submit(() -> {
          queue.removeDispatchedEvents(eventID);
        }));
      }

      for (Future<Void> future : futures) {
        future.get(getTimeout().toMillis(), MILLISECONDS);
      }
    }
  }

  @Test
  public void verifyPutEntryConditionallyInHAContainerNoOverwrite() throws Exception {
    // create message and HAEventWrapper
    EventID eventID = new EventID(cache.getDistributedSystem());

    ClientUpdateMessage oldMessage = new ClientUpdateMessageImpl(EnumListenerEvent.AFTER_UPDATE,
        (LocalRegion) dataRegion, "key", "value".getBytes(), (byte) 0x01, null,
        new ClientProxyMembershipID(), eventID);

    HAEventWrapper originalWrapperInstance = new HAEventWrapper(oldMessage);
    originalWrapperInstance.incrementPutInProgressCounter("test");

    HAContainerWrapper haContainerWrapper = new HAContainerMap(new ConcurrentHashMap());
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
    newWrapperInstance.incrementPutInProgressCounter("test");
    newWrapperInstance.setHAContainer(haContainerWrapper);

    haRegionQueue.putEventInHARegion(newWrapperInstance, 1L);

    // Add the original wrapper back in, and verify that it does not overwrite the new one
    // and that it increments the ref count on the container key.
    haRegionQueue.putEventInHARegion(originalWrapperInstance, 1L);

    assertThat(newWrapperInstance.getClientUpdateMessage())
        .withFailMessage("Original message overwrote new message in container")
        .isEqualTo(haContainerWrapper.get(originalWrapperInstance));

    assertThat(newWrapperInstance.getReferenceCount())
        .withFailMessage("Reference count was not the expected value")
        .isEqualTo(2);

    assertThat(haContainerWrapper)
        .withFailMessage("Container size was not the expected value")
        .hasSize(1);
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
    haEventWrapper.incrementPutInProgressCounter("test");
    haEventWrapper.setHAContainer(haContainerWrapper);

    haRegionQueue.put(haEventWrapper);

    List<Future<Void>> futures = new ArrayList<>();

    // In one thread, simulate processing a queue removal message
    // by removing the dispatched event
    futures.add(executorServiceRule.submit(() -> {
      haRegionQueue.removeDispatchedEvents(eventID);
    }));

    // In another thread, simulate that the region is being destroyed, for instance
    // when a SocketTimeoutException is thrown and we are cleaning up
    futures.add(executorServiceRule.submit(haRegionQueue::destroy));

    for (Future<Void> future : futures) {
      future.get();
    }

    await().untilAsserted(() -> {
      assertThat(haEventWrapper.getReferenceCount())
          .withFailMessage(
              "Expected HAEventWrapper reference count to be decremented to 0 by either the queue removal or destroy queue logic")
          .isZero();
    });
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
    haEventWrapper.incrementPutInProgressCounter("test");
    haEventWrapper.setHAContainer(haContainerWrapper);

    haRegionQueue.put(haEventWrapper);

    List<Future<Void>> futures = new ArrayList<>();

    // In one thread, simulate processing a queue removal message
    // by removing the dispatched event
    futures.add(executorServiceRule.submit(() -> {
      // Simulate dispatching a message by peeking and removing the HAEventWrapper
      haRegionQueue.peek();
      haRegionQueue.remove();
    }));

    // In another thread, simulate that the region is being destroyed, for instance
    // when a SocketTimeoutException is thrown and we are cleaning up
    futures.add(executorServiceRule.submit(haRegionQueue::destroy));

    for (Future<Void> future : futures) {
      future.get();
    }

    await().untilAsserted(() -> {
      assertThat(haEventWrapper.getReferenceCount())
          .withFailMessage(
              "Expected HAEventWrapper reference count to be decremented to 0 by either the message dispatcher or destroy queue logic")
          .isZero();
    });
  }

  private InternalCache createCache() {
    return (InternalCache) new CacheFactory().set(MCAST_PORT, "0").create();
  }

  private Region createDataRegion() {
    return cache.createRegionFactory(RegionShortcut.REPLICATE).create("data");
  }

  private CacheClientNotifier createCacheClientNotifier() {
    CacheClientNotifier ccn =
        CacheClientNotifier.getInstance(cache,
            mock(ClientRegistrationEventQueueManager.class),
            mock(StatisticsClock.class),
            mock(CacheServerStats.class),
            100000,
            100000,
            mock(ConnectionListener.class),
            null,
            false);
    return ccn;
  }

  private InternalDistributedMember createMember() {
    // Create an InternalDistributedMember
    InternalDistributedMember member = mock(InternalDistributedMember.class);
    when(member.getVersion()).thenReturn(KnownVersion.CURRENT);
    return member;
  }

  private HARegion createMockHARegion() {
    HARegion haRegion = mock(HARegion.class);
    Map<Object, Object> map = new ConcurrentHashMap<>();

    when(haRegion.getGemFireCache())
        .thenReturn(cache);
    when(haRegion.put(any(Object.class), any(Object.class)))
        .then(answer -> map.put(answer.getArgument(0), answer.getArgument(1)));
    when(haRegion.get(any(Object.class)))
        .then(answer -> map.get(answer.getArgument(0)));

    doAnswer(answer -> {
      map.remove(answer.getArgument(0));
      return null;
    }).when(haRegion).localDestroy(any(Object.class));

    return haRegion;
  }

  private HAContainerRegion createHAContainerRegion() throws IOException, ClassNotFoundException {
    return new HAContainerRegion(createHAContainerRegionRegion());
  }

  private Region<Object, Object> createHAContainerRegionRegion()
      throws IOException, ClassNotFoundException {
    String regionName = CacheServerImpl.generateNameForClientMsgsRegion(0);

    AttributesFactory<Object, Object> factory = new AttributesFactory<>();
    factory.setDataPolicy(DataPolicy.NORMAL);
    factory.setDiskStoreName(null);
    factory.setDiskSynchronous(true);
    factory.setEvictionAttributes(OVERFLOW_TO_DISK);
    factory.setStatisticsEnabled(true);
    factory.setScope(Scope.LOCAL);

    InternalRegionArguments arguments = new InternalRegionArguments()
        .setDestroyLockFlag(true)
        .setRecreateFlag(false)
        .setSnapshotInputStream(null)
        .setImageTarget(null)
        .setIsUsedForMetaRegion(true);

    return cache.createVMRegion(regionName, factory.create(), arguments);
  }

  private HARegionQueue createHARegionQueue(Map haContainer, int index, HARegion haRegion,
      boolean puttingGIIDataInQueue)
      throws InterruptedException, IOException, ClassNotFoundException {
    StoppableReentrantReadWriteLock giiLock = mock(StoppableReentrantReadWriteLock.class);
    StoppableReentrantReadWriteLock rwLock =
        new StoppableReentrantReadWriteLock(cache.getCancelCriterion());

    when(giiLock.writeLock()).thenReturn(mock(StoppableWriteLock.class));
    when(giiLock.readLock()).thenReturn(mock(StoppableReadLock.class));

    return new HARegionQueue("haRegion+" + index, haRegion, cache, haContainer,
        null, (byte) 1, true, mock(HARegionQueueStats.class), giiLock, rwLock,
        mock(CancelCriterion.class), puttingGIIDataInQueue, mock(StatisticsClock.class));
  }

  private HARegionQueue createHARegionQueue(Map haContainer, int index)
      throws InterruptedException, IOException, ClassNotFoundException {
    HARegion haRegion = mock(HARegion.class);

    when(haRegion.getGemFireCache()).thenReturn(cache);

    return createHARegionQueue(haContainer, index, haRegion, false);
  }

  private CachedDeserializable createCachedDeserializable(HAContainerWrapper haContainerWrapper)
      throws IOException {
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
    assertThat(haContainerWrapper).isEmpty();
    return cd;
  }

  private void createAndUpdateHARegionQueuesSequentially(HAContainerWrapper haContainerWrapper,
      CachedDeserializable cd, int numQueues)
      throws InterruptedException, IOException, ClassNotFoundException {
    // Create some HARegionQueues
    for (int i = 0; i < numQueues; i++) {
      HARegionQueue haRegionQueue = createHARegionQueue(haContainerWrapper, i);
      haRegionQueue.updateHAEventWrapper(member, cd, "haRegion");
    }
  }

  private HARegionQueue createAndUpdateHARegionQueuesWithGiiQueueing(
      HAContainerWrapper haContainerWrapper, HAEventWrapper wrapper, int numQueues)
      throws InterruptedException, IOException, ClassNotFoundException {
    HARegionQueue targetQueue = null;
    int startGiiQueueingIndex = numQueues / 2;

    // create HARegionQueues and startGiiQueuing on a region about half way through
    for (int i = 0; i < numQueues; i++) {
      HARegionQueue haRegionQueue = null;

      // start GII Queueing (targetRegionQueue)
      if (i == startGiiQueueingIndex) {
        HARegion haRegion = mock(HARegion.class);

        HARegionQueue giiHaRegionQueue =
            createHARegionQueue(haContainerWrapper, i, haRegion, false);
        giiHaRegionQueue.startGiiQueueing();
        targetQueue = giiHaRegionQueue;

        when(haRegion.put(any(Object.class), any(HAEventWrapper.class)))
            .then(answer -> {
              // Simulate that either a QRM or message dispatch has occurred immediately after the
              // put.
              // We want to ensure that the event is removed from the HAContainer if it is drained
              // from the giiQueue and the ref count has dropped to 0.
              HAEventWrapper haContainerKey = answer.getArgument(1);
              giiHaRegionQueue.decAndRemoveFromHAContainer(haContainerKey);
              return null;
            });

        when(haRegion.getGemFireCache()).thenReturn(cache);

        haRegionQueue = giiHaRegionQueue;
      } else {
        haRegionQueue = createHARegionQueue(haContainerWrapper, i);
      }

      haRegionQueue.put(wrapper);
    }

    return targetQueue;
  }

  private Set<HAEventWrapper> createAndPutHARegionQueuesSimultaneously(
      HAContainerWrapper haContainerWrapper, int numQueues, int numOperations)
      throws InterruptedException, IOException, ClassNotFoundException {
    Collection<HARegionQueue> queues = new ConcurrentLinkedQueue<>();
    AtomicInteger count = new AtomicInteger();

    // create HARegionQueues
    for (int i = 0; i < numQueues; i++) {
      queues.add(createHARegionQueue(haContainerWrapper, i));
    }

    Set<HAEventWrapper> testValidationWrapperSet = ConcurrentHashMap.newKeySet();
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
      HAEventWrapper haEventWrapper, int numQueues)
      throws InterruptedException, IOException, ClassNotFoundException {
    Collection<HARegionQueue> queues = new ArrayList<>();

    // create HARegionQueues
    for (int i = 0; i < numQueues; i++) {
      queues.add(createHARegionQueue(haContainerWrapper, i));
    }

    haEventWrapper.incrementPutInProgressCounter("test");

    for (HARegionQueue queue : queues) {
      queue.put(haEventWrapper);
    }

    haEventWrapper.decrementPutInProgressCounter();
  }

  private void createAndUpdateHARegionQueuesSimultaneously(HAContainerWrapper haContainerWrapper,
      CachedDeserializable cd, int numQueues)
      throws InterruptedException, IOException, ClassNotFoundException, TimeoutException,
      ExecutionException {
    // Create some HARegionQueues
    HARegionQueue[] haRegionQueues = new HARegionQueue[numQueues];
    for (int i = 0; i < numQueues; i++) {
      haRegionQueues[i] = createHARegionQueue(haContainerWrapper, i);
    }

    // Create threads to simultaneously update the HAEventWrapper
    Collection<Future<Void>> futures = new ArrayList<>();
    for (HARegionQueue haRegionQueue : haRegionQueues) {
      futures.add(executorServiceRule.submit(() -> {
        haRegionQueue.updateHAEventWrapper(member, cd, "haRegion");
      }));
    }

    for (Future<Void> future : futures) {
      future.get(getTimeout().toMillis(), MILLISECONDS);
    }
  }

  private void verifyHAContainerWrapper(HAContainerWrapper haContainerWrapper,
      CachedDeserializable cd, int numQueues) {
    // Verify HAContainerRegion size
    assertThat(haContainerWrapper).hasSize(1);

    // Verify the refCount is correct
    HAEventWrapper wrapperInContainer =
        (HAEventWrapper) haContainerWrapper.getKey(cd.getDeserializedForReading());
    assertThat(wrapperInContainer.getReferenceCount()).isEqualTo(numQueues);
  }

  private static <T> T mock(Class<T> classToMock) {
    return Mockito.mock(classToMock, withSettings().stubOnly());
  }
}
