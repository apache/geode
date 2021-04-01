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
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.test.fake.Fakes;

public class DistributedRegionSearchLoadJUnitTest {

  protected DistributedRegion createAndDefineRegion(boolean isConcurrencyChecksEnabled,
      RegionAttributes<Object, Object> ra, InternalRegionArguments ira, GemFireCacheImpl cache) {
    DistributedRegion region =
        new DistributedRegion("testRegion", ra, null, cache, ira, disabledClock());
    if (isConcurrencyChecksEnabled) {
      region.enableConcurrencyChecks();
    }

    // since it is a real region object, we need to tell mockito to monitor it
    region = spy(region);

    doNothing().when(region).distributeUpdate(any(), anyLong(), anyBoolean(), anyBoolean(), any(),
        anyBoolean());
    doNothing().when(region).distributeDestroy(any(), any());
    doNothing().when(region).distributeInvalidate(any());
    doNothing().when(region).distributeUpdateEntryVersion(any());
    return region;
  }

  private RegionAttributes<Object, Object> createRegionAttributes(
      boolean isConcurrencyChecksEnabled) {
    AttributesFactory<Object, Object> factory = new AttributesFactory<>();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(isConcurrencyChecksEnabled);
    return factory.create();
  }

  private EventID createDummyEventID() {
    byte[] memId = {1, 2, 3};
    return new EventID(memId, 11, 12, 13);
  }

  protected EntryEventImpl createDummyEvent(DistributedRegion region) {
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

  protected VersionTag<InternalDistributedMember> createVersionTag(boolean validVersionTag) {
    InternalDistributedMember remoteMember = mock(InternalDistributedMember.class);
    VersionTag<InternalDistributedMember> tag = uncheckedCast(VersionTag.create(remoteMember));
    if (validVersionTag) {
      tag.setRegionVersion(1);
      tag.setEntryVersion(1);
    }
    return tag;
  }

  protected DistributedRegion prepare(boolean isConcurrencyChecksEnabled) {
    GemFireCacheImpl cache = Fakes.cache();

    // create region attributes and internal region arguments
    RegionAttributes<Object, Object> ra = createRegionAttributes(isConcurrencyChecksEnabled);
    InternalRegionArguments ira = new InternalRegionArguments();

    // create a region object
    DistributedRegion region = createAndDefineRegion(isConcurrencyChecksEnabled, ra, ira, cache);
    if (isConcurrencyChecksEnabled) {
      region.enableConcurrencyChecks();
    }

    doNothing().when(region).notifyGatewaySender(any(), any());
    doReturn(true).when(region).hasSeenEvent(any(EntryEventImpl.class));
    return region;
  }

  private void createSearchLoad(DistributedRegion region) {
    SearchLoadAndWriteProcessor proc = mock(SearchLoadAndWriteProcessor.class);

    doAnswer((Answer<EntryEventImpl>) invocation -> {
      Object[] args = invocation.getArguments();
      if (args[0] instanceof EntryEventImpl) {
        EntryEventImpl event = (EntryEventImpl) args[0];
        event.setNewValue("NewLoadedValue");
        event.setOperation(Operation.LOCAL_LOAD_CREATE);
      }
      return null;
    }).when(proc).doSearchAndLoad(any(EntryEventImpl.class), any(), any(),
        anyBoolean());

    doReturn(proc).when(region).getSearchLoadAndWriteProcessor();
  }

  @Test
  public void testClientEventIsUpdatedWithCurrentEntryVersionTagAfterLoad() {
    DistributedRegion region = prepare(true);
    EntryEventImpl event = createDummyEvent(region);
    region.basicInvalidate(event);

    createSearchLoad(region);

    KeyInfo ki = new KeyInfo(event.getKey(), null, null);
    region.findObjectInSystem(ki, false, null, false, null, false, false, null, event, false);
    assertNotNull("ClientEvent version tag is not set with region version tag.",
        event.getVersionTag());
  }

  @Test
  public void testClientEventIsUpdatedWithCurrentEntryVersionTagAfterSearchConcurrencyException() {
    DistributedRegion region = prepare(true);

    EntryEventImpl event = createDummyEvent(region);
    region.basicInvalidate(event);

    VersionTag<InternalDistributedMember> tag = createVersionTag(true);
    RegionEntry re = mock(RegionEntry.class);
    VersionStamp stamp = mock(VersionStamp.class);

    doReturn(re).when(region).getRegionEntry(any());
    when(re.getVersionStamp()).thenReturn(stamp);
    when(stamp.asVersionTag()).thenReturn(tag);

    createSearchLoad(region);
    doThrow(new ConcurrentCacheModificationException()).when(region)
        .basicPutEntry(any(EntryEventImpl.class), anyLong());

    KeyInfo ki = new KeyInfo(event.getKey(), null, null);
    region.findObjectInSystem(ki, false, null, false, null, false, false, null, event, false);
    assertNotNull("ClientEvent version tag is not set with region version tag.",
        event.getVersionTag());
  }

}
