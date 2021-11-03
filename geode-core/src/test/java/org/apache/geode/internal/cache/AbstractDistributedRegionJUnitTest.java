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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import org.apache.logging.log4j.Logger;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.fake.Fakes;

public abstract class AbstractDistributedRegionJUnitTest {

  protected static final Logger logger = LogService.getLogger();

  private RegionAttributes createRegionAttributes(boolean isConcurrencyChecksEnabled) {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(isConcurrencyChecksEnabled); //
    RegionAttributes ra = factory.create();
    return ra;
  }

  private EventID createDummyEventID() {
    byte[] memId = {1, 2, 3};
    EventID eventId = new EventID(memId, 11, 12, 13);
    return eventId;
  }

  private EntryEventImpl createDummyEvent(DistributedRegion region) {
    // create a dummy event id
    EventID eventId = createDummyEventID();
    String key = "key1";
    String value = "Value1";

    // create an event
    EntryEventImpl event = EntryEventImpl.create(region, Operation.CREATE, key, value, null,
        false /* origin remote */, null, false /* generateCallbacks */, eventId);
    // avoid calling invokeCallbacks
    event.callbacksInvoked(true);

    return event;
  }

  private VersionTag createVersionTag(boolean validVersionTag) {
    InternalDistributedMember remotemember = mock(InternalDistributedMember.class);
    VersionTag tag = VersionTag.create(remotemember);
    if (validVersionTag) {
      tag.setRegionVersion(1);
      tag.setEntryVersion(1);
    }
    return tag;
  }

  private void doTest(DistributedRegion region, EntryEventImpl event, int cnt) {
    // do the virtualPut test
    verifyDistributeUpdate(region, event, cnt);

    // do the basicDestroy test
    verifyDistributeDestroy(region, event, cnt);

    // do the basicInvalidate test
    verifyDistributeInvalidate(region, event, cnt);

    // do the basicUpdateEntryVersion test
    verifyDistributeUpdateEntryVersion(region, event, cnt);
  }

  protected abstract void setInternalRegionArguments(InternalRegionArguments ira);

  protected abstract DistributedRegion createAndDefineRegion(boolean isConcurrencyChecksEnabled,
      RegionAttributes ra, InternalRegionArguments ira, GemFireCacheImpl cache,
      StatisticsClock statisticsClock);

  protected abstract void verifyDistributeUpdate(DistributedRegion region, EntryEventImpl event,
      int cnt);

  protected abstract void verifyDistributeDestroy(DistributedRegion region, EntryEventImpl event,
      int cnt);

  protected abstract void verifyDistributeInvalidate(DistributedRegion region, EntryEventImpl event,
      int cnt);

  protected abstract void verifyDistributeUpdateEntryVersion(DistributedRegion region,
      EntryEventImpl event, int cnt);

  protected DistributedRegion prepare(boolean isConcurrencyChecksEnabled,
      boolean testHasSeenEvent) {
    GemFireCacheImpl cache = Fakes.cache();

    // create region attributes and internal region arguments
    RegionAttributes ra = createRegionAttributes(isConcurrencyChecksEnabled);
    InternalRegionArguments ira = new InternalRegionArguments();

    setInternalRegionArguments(ira);

    // create a region object
    DistributedRegion region =
        createAndDefineRegion(isConcurrencyChecksEnabled, ra, ira, cache, disabledClock());
    if (isConcurrencyChecksEnabled) {
      region.enableConcurrencyChecks();
    }

    doNothing().when(region).notifyGatewaySender(any(), any());
    if (!testHasSeenEvent) {
      doReturn(true).when(region).hasSeenEvent(any(EntryEventImpl.class));
    }
    return region;
  }

  @Test
  public void testConcurrencyFalseTagNull() {
    // case 1: concurrencyCheckEanbled = false, version tag is null: distribute
    DistributedRegion region = prepare(false, false);
    EntryEventImpl event = createDummyEvent(region);
    assertNull(event.getVersionTag());
    doTest(region, event, 1);
  }

  @Test
  public void testConcurrencyTrueTagNull() {
    // case 2: concurrencyCheckEanbled = true, version tag is null: not to distribute
    DistributedRegion region = prepare(true, false);
    EntryEventImpl event = createDummyEvent(region);
    assertNull(event.getVersionTag());
    doTest(region, event, 0);
  }

  @Test
  public void testConcurrencyTrueTagInvalid() {
    // case 3: concurrencyCheckEanbled = true, version tag is invalid: not to distribute
    DistributedRegion region = prepare(true, false);
    EntryEventImpl event = createDummyEvent(region);
    VersionTag tag = createVersionTag(false);
    event.setVersionTag(tag);
    assertFalse(tag.hasValidVersion());
    doTest(region, event, 0);
  }

  @Test
  public void testConcurrencyTrueTagValid() {
    // case 4: concurrencyCheckEanbled = true, version tag is valid: distribute
    DistributedRegion region = prepare(true, false);
    EntryEventImpl event = createDummyEvent(region);
    VersionTag tag = createVersionTag(true);
    event.setVersionTag(tag);
    assertTrue(tag.hasValidVersion());
    doTest(region, event, 1);
  }
}
