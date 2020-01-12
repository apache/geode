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

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.AFTER_INITIAL_IMAGE;
import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.ANY_INIT;
import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.BEFORE_INITIAL_IMAGE;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheClientProxy;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientRegistrationEventQueueManager;
import org.apache.geode.internal.cache.tier.sockets.ConnectionListener;

public class LocalRegionPartialMockTest {
  private LocalRegion region;
  private EntryEventImpl event;
  private ServerRegionProxy serverRegionProxy;
  private Operation operation;
  private CancelCriterion cancelCriterion;
  private RegionAttributes regionAttributes;
  private InternalCache cache;
  private CacheClientProxy proxy;

  private final Object key = new Object();
  private final String value = "value";

  @Before
  public void setup() {
    region = mock(LocalRegion.class);
    event = mock(EntryEventImpl.class);
    serverRegionProxy = mock(ServerRegionProxy.class);
    cancelCriterion = mock(CancelCriterion.class);
    regionAttributes = mock(RegionAttributes.class);
    cache = mock(InternalCache.class);
    proxy = mock(CacheClientProxy.class);

    when(region.getServerProxy()).thenReturn(serverRegionProxy);
    when(event.isFromServer()).thenReturn(false);
    when(event.getKey()).thenReturn(key);
    when(event.getRawNewValue()).thenReturn(value);
    when(region.getCancelCriterion()).thenReturn(cancelCriterion);
    when(region.getAttributes()).thenReturn(regionAttributes);
    when(region.getCache()).thenReturn(cache);
  }

  @Test
  public void serverPutWillCheckPutIfAbsentResult() {
    Object result = new Object();
    operation = Operation.PUT_IF_ABSENT;
    when(event.getOperation()).thenReturn(operation);
    when(event.isCreate()).thenReturn(true);
    when(serverRegionProxy.put(key, value, null, event, operation, true, null, null, true))
        .thenReturn(result);
    doCallRealMethod().when(region).serverPut(event, true, null);

    region.serverPut(event, true, null);

    verify(region).checkPutIfAbsentResult(event, value, result);
  }

  @Test
  public void checkPutIfAbsentResultSucceedsIfResultIsNull() {
    Object result = null;
    doCallRealMethod().when(region).checkPutIfAbsentResult(event, value, result);

    region.checkPutIfAbsentResult(event, value, result);

    verify(event, never()).hasRetried();
  }

  @Test(expected = EntryNotFoundException.class)
  public void checkPutIfAbsentResultThrowsIfResultNotNullAndEventHasNotRetried() {
    Object result = new Object();
    when(event.hasRetried()).thenReturn(false);
    doCallRealMethod().when(region).checkPutIfAbsentResult(event, value, result);

    region.checkPutIfAbsentResult(event, value, result);
  }

  @Test
  public void testNotifiesSerialGatewaySenderEmptySenders() {
    doCallRealMethod().when(region).notifiesSerialGatewaySender();
    assertThat(region.notifiesSerialGatewaySender()).isFalse();
  }

  @Test
  public void testNotifiesSerialGatewaySenderPdxRegion() {
    when(region.isPdxTypesRegion()).thenReturn(false);
    doCallRealMethod().when(region).notifiesSerialGatewaySender();
    assertThat(region.notifiesSerialGatewaySender()).isFalse();
  }

  @Test
  public void testNotifiesSerialGatewaySenderWithSerialGatewaySender() {
    createGatewaySender("sender1", false);
    doCallRealMethod().when(region).notifiesSerialGatewaySender();
    assertThat(region.notifiesSerialGatewaySender()).isTrue();
  }

  @Test
  public void testNotifiesSerialGatewaySenderWithParallelGatewaySender() {
    createGatewaySender("sender1", true);
    doCallRealMethod().when(region).notifiesSerialGatewaySender();
    assertThat(region.notifiesSerialGatewaySender()).isFalse();
  }

  @Test
  public void testInitializationThreadInitValue() throws InterruptedException, ExecutionException {
    ExecutorService executionService = Executors.newFixedThreadPool(1);
    Future future =
        executionService.submit(() -> assertThat(LocalRegion.getThreadInitLevelRequirement())
            .isEqualTo(AFTER_INITIAL_IMAGE));
    future.get();
  }

  @Test
  public void testSetThreadInitLevelRequirement() throws InterruptedException, ExecutionException {
    ExecutorService executionService = Executors.newFixedThreadPool(1);
    Future future = executionService.submit(() -> {
      assertThat(LocalRegion.setThreadInitLevelRequirement(ANY_INIT))
          .isEqualTo(AFTER_INITIAL_IMAGE);
      assertThat(LocalRegion.getThreadInitLevelRequirement()).isEqualTo(ANY_INIT);
      assertThat(LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE))
          .isEqualTo(ANY_INIT);
      assertThat(LocalRegion.getThreadInitLevelRequirement()).isEqualTo(BEFORE_INITIAL_IMAGE);
      assertThat(LocalRegion.setThreadInitLevelRequirement(AFTER_INITIAL_IMAGE))
          .isEqualTo(BEFORE_INITIAL_IMAGE);
      assertThat(LocalRegion.getThreadInitLevelRequirement()).isEqualTo(AFTER_INITIAL_IMAGE);
    });
    future.get();
  }

  @Test
  public void testSetThreadInitLevelRequirementDoesNotAffectOtherThreads()
      throws InterruptedException, ExecutionException {
    ExecutorService executionService1 = Executors.newFixedThreadPool(1);
    Future future1 = executionService1.submit(() -> {
      assertThat(LocalRegion.setThreadInitLevelRequirement(ANY_INIT))
          .isEqualTo(AFTER_INITIAL_IMAGE);
      assertThat(LocalRegion.getThreadInitLevelRequirement()).isEqualTo(ANY_INIT);
    });
    future1.get();
    ExecutorService executionService2 = Executors.newFixedThreadPool(1);
    Future future2 =
        executionService2.submit(() -> assertThat(LocalRegion.getThreadInitLevelRequirement())
            .isEqualTo(AFTER_INITIAL_IMAGE));
    future2.get();
  }

  private void createGatewaySender(String senderId, boolean isParallel) {
    // Create set of sender ids
    Set<String> allGatewaySenderIds = Stream.of(senderId).collect(Collectors.toSet());
    when(region.getAllGatewaySenderIds()).thenReturn(allGatewaySenderIds);

    // Create set of senders
    GatewaySender sender = mock(GatewaySender.class);
    when(sender.getId()).thenReturn(senderId);
    when(sender.isParallel()).thenReturn(isParallel);
    Set<GatewaySender> allGatewaySenders = Stream.of(sender).collect(Collectors.toSet());
    when(cache.getAllGatewaySenders()).thenReturn(allGatewaySenders);
  }

  @Test
  public void testDoNotNotifyClientsOfTombstoneGCNoCacheClientNotifier() {

    Map regionGCVersions = new HashMap();
    Set keysRemoved = new HashSet();
    EventID eventID = new EventID();
    FilterInfo routing = new FilterInfo();

    doCallRealMethod().when(region).notifyClientsOfTombstoneGC(regionGCVersions, keysRemoved,
        eventID, routing);
    region.notifyClientsOfTombstoneGC(regionGCVersions, keysRemoved, eventID, routing);
    verify(region, never()).getFilterProfile();
  }

  @Test
  public void testDoNotNotifyClientsOfTombstoneGCNoProxy() {

    Map regionGCVersions = new HashMap();
    Set keysRemoved = new HashSet();
    EventID eventID = new EventID();
    FilterInfo routing = null;
    when(cache.getCCPTimer()).thenReturn(mock(SystemTimer.class));

    CacheClientNotifier ccn =
        CacheClientNotifier.getInstance(cache, mock(ClientRegistrationEventQueueManager.class),
            disabledClock(), mock(CacheServerStats.class), 10,
            10, mock(ConnectionListener.class), null, true);

    doCallRealMethod().when(region).notifyClientsOfTombstoneGC(regionGCVersions, keysRemoved,
        eventID, routing);
    region.notifyClientsOfTombstoneGC(regionGCVersions, keysRemoved, eventID, routing);
    verify(region, never()).getFilterProfile();

    when(cache.getCacheServers()).thenReturn(Collections.emptyList());
    ccn.shutdown(111);
  }

  @Test
  public void testNotifyClientsOfTombstoneGC() {

    Map regionGCVersions = new HashMap();
    Set keysRemoved = new HashSet();
    EventID eventID = new EventID();
    FilterInfo routing = null;
    when(cache.getCCPTimer()).thenReturn(mock(SystemTimer.class));

    CacheClientNotifier ccn =
        CacheClientNotifier.getInstance(cache, mock(ClientRegistrationEventQueueManager.class),
            disabledClock(), mock(CacheServerStats.class), 10,
            10, mock(ConnectionListener.class), null, true);

    when(proxy.getProxyID()).thenReturn(mock(ClientProxyMembershipID.class));
    ccn.addClientProxyToMap(proxy);
    doCallRealMethod().when(region).notifyClientsOfTombstoneGC(regionGCVersions, keysRemoved,
        eventID, routing);
    when(region.getFilterProfile()).thenReturn(null);

    region.notifyClientsOfTombstoneGC(regionGCVersions, keysRemoved, eventID, routing);

    when(cache.getCacheServers()).thenReturn(Collections.emptyList());
    when(proxy.getAcceptorId()).thenReturn(Long.valueOf(111));

    ccn.shutdown(111);
  }
}
