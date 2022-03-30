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

import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.RegisterInterestTracker;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.distributed.internal.DSClock;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.tier.InterestType;

public class LocalRegionUpdateTest {
  private InternalDataView internalDataView;
  private LocalRegion region;
  private RegisterInterestTracker registerInterestTracker;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Before
  public void setUp() {
    internalDataView = mock(InternalDataView.class);

    EntryEventFactory entryEventFactory = mock(EntryEventFactory.class);
    InternalCache cache = mock(InternalCache.class);
    InternalDistributedSystem internalDistributedSystem = mock(InternalDistributedSystem.class);
    InternalRegionArguments internalRegionArguments = mock(InternalRegionArguments.class);

    LocalRegion.RegionMapConstructor regionMapConstructor =
        mock(LocalRegion.RegionMapConstructor.class);
    Function<LocalRegion, RegionPerfStats> regionPerfStatsFactory = localRegion -> {
      localRegion.getLocalSize();
      return mock(RegionPerfStats.class);
    };

    final PoolImpl poolImpl = mock(PoolImpl.class);
    SubscriptionAttributes subscriptionAttributes =
        new SubscriptionAttributes(InterestPolicy.ALL);


    when(cache.getInternalDistributedSystem()).thenReturn(internalDistributedSystem);
    when(internalDistributedSystem.getClock()).thenReturn(mock(DSClock.class));

    when(regionMapConstructor.create(any(), any(), any())).thenReturn(mock(RegionMap.class));

    InternalRegionFactory<Object, Object> regionFactory = new InternalRegionFactory<>(cache);
    regionFactory.setDataPolicy(DataPolicy.NORMAL);
    regionFactory.setPoolName("Pool1");
    regionFactory.setConcurrencyChecksEnabled(false);
    regionFactory.setSubscriptionAttributes(subscriptionAttributes);
    RegionAttributes<Object, Object> regionAttributes =
        regionFactory.getCreateAttributes();

    registerInterestTracker = new RegisterInterestTracker();
    when(poolImpl.getRITracker()).thenReturn(registerInterestTracker);

    ServerRegionProxy serverRegionProxy = mock(ServerRegionProxy.class);
    when(serverRegionProxy.getPool()).thenReturn(poolImpl);
    LocalRegion.ServerRegionProxyConstructor proxyConstructor = region -> serverRegionProxy;

    AbstractRegion.PoolFinder poolFinder = poolName -> poolImpl;

    region = spy(new LocalRegion("region", regionAttributes, null, cache,
        internalRegionArguments, internalDataView, regionMapConstructor, proxyConstructor,
        entryEventFactory, poolFinder, regionPerfStatsFactory, disabledClock()));

  }

  /*
   * As indicated by the name this code tests the basicBridgeClientUpdate method's
   * fork where it checks to see if the interestResultPolicy is NONE, then it will
   * call basicUpdate.
   */
  @Test
  public void basicBridgeClientUpdateChecksInterestResultPolicyNoneThenUpdates() {
    Object key = new Object();
    Object value = new Object();

    registerInterestTracker.addSingleInterest(region, key, InterestType.KEY,
        InterestResultPolicy.NONE, true, false);

    region.basicBridgeClientUpdate(null, key, value, new byte[] {'0'}, true,
        null, true, false, new EntryEventImpl(),
        new EventID(new byte[] {1}, 1, 1));

    verify(internalDataView).putEntry(any(), anyBoolean(),
        anyBoolean(), any(), anyBoolean(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean());

  }

  /*
   * As indicated by the name this code tests the basicBridgeClientUpdate method's
   * fork where it checks to see if the interestResultPolicy is KEY, then it will
   * not call basicUpdate.
   */
  @Test
  public void basicBridgeClientUpdateChecksInterestResultPolicyKeyThenDoesNotUpdate() {
    Object key = new Object();
    Object value = new Object();

    registerInterestTracker.addSingleInterest(region, key, InterestType.KEY,
        InterestResultPolicy.KEYS, true, false);

    region.basicBridgeClientUpdate(null, key, value, new byte[] {'0'}, true,
        null, true, false, new EntryEventImpl(),
        new EventID(new byte[] {1}, 1, 1));

    verify(internalDataView, times(0)).putEntry(any(), anyBoolean(),
        anyBoolean(), any(), anyBoolean(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean());

  }

}
