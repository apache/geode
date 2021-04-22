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

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.DefaultEntryEventFactory;
import org.apache.geode.internal.cache.EntryEventFactory;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.versions.VersionTag;

public class TimestampAwareConflationDUnitTest extends WANTestBase {
  static VersionTag oldVersionTag;

  @After
  public void cleanup() {
    oldVersionTag = null;
  }


  @Test
  public void testSerialGatewaySender() {

    String mykey = "mykey";
    String regionName = getTestMethodName();
    String senderName = "senderTo2";

    Integer site1Port = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    vm1.invoke(() -> createFirstRemoteLocator(2, site1Port));

    vm4.invoke(() -> System.setProperty(LOG_LEVEL, "debug")); //////////

    createCacheInVMs(site1Port, vm4, vm5);

    vm4.invoke(() -> createPartitionedRegion(regionName, senderName, 1, 8, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(regionName, senderName, 1, 8, isOffHeap()));

    vm4.invoke(() -> createSender(senderName, 2, false, 100, 50, true, false, null, true));
    vm5.invoke(() -> createSender(senderName, 2, false, 100, 50, true, false, null, true));

    startSenderInVMs(senderName, vm4, vm5);

    // Paused, or peek will not return all the events
    vm4.invoke(() -> pauseSender(senderName));
    vm5.invoke(() -> pauseSender(senderName));

    vm4.invoke(() -> doPutsSameKeyFrom(regionName, 5, 0, mykey));

    vm4.invoke(() -> saveLatestVersionTag(regionName, mykey));

    vm4.invoke(() -> doPutsSameKeyFrom(regionName, 2, 5, mykey));

    List<Integer> remoteDSs = new ArrayList<>();
    remoteDSs.add(2);

    vm4.invoke(
        () -> enqueueEventWithOlderTimestamp(senderName, remoteDSs, regionName, mykey, "Value_4"));

    vm4.invoke(() -> verifyOlderSerialEventWasNotConflated(senderName));

  }

  @Test
  public void testParallelGatewaySender() {

    String mykey = "mykey";
    String regionName = getTestMethodName();
    String senderName = "senderTo2";

    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm1.invoke(() -> createFirstRemoteLocator(2, lnPort));
    vm4.invoke(() -> System.setProperty(LOG_LEVEL, "debug"));
    createCacheInVMs(lnPort, vm4, vm5);

    vm4.invoke(() -> createPartitionedRegion(regionName, senderName, 1, 8, isOffHeap()));
    vm5.invoke(() -> createPartitionedRegion(regionName, senderName, 1, 8, isOffHeap()));

    vm4.invoke(() -> createSender(senderName, 2, true, 100, 50, true, false, null, true));
    vm5.invoke(() -> createSender(senderName, 2, true, 100, 50, true, false, null, true));

    startSenderInVMs(senderName, vm4, vm5);

    // Paused, or peek will not return all the events
    vm4.invoke(() -> pauseSender(senderName));
    vm5.invoke(() -> pauseSender(senderName));

    vm4.invoke(() -> doPutsSameKeyFrom(regionName, 5, 0, mykey));

    vm4.invoke(() -> saveLatestVersionTag(regionName, mykey));

    vm4.invoke(() -> doPutsSameKeyFrom(regionName, 2, 5, mykey));

    List<Integer> remoteDSs = new ArrayList<>();
    remoteDSs.add(2);

    vm4.invoke(
        () -> enqueueEventWithOlderTimestamp(senderName, remoteDSs, regionName, mykey, "Value_4"));

    vm4.invoke(() -> verifyOlderParallelEventWasNotConflated(senderName));
  }



  void verifyOlderParallelEventWasNotConflated(String senderName) throws InterruptedException {
    AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderName);
    assertThat(sender.getQueue().size()).isEqualTo(3); // CREATE, UPDATE & OLD EVENT

    List<String> eventValues = new ArrayList<>(3);
    for (Object o : sender.getQueue().getRegion().values()) {
      eventValues.add(((GatewaySenderEventImpl) o).getValueAsString(true));
    }

    assertThat(eventValues).contains("Value_0");
    assertThat(eventValues).contains("Value_4");
    assertThat(eventValues).contains("Value_6");
  }

  void verifyOlderSerialEventWasNotConflated(String senderName) throws InterruptedException {
    AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderName);
    assertThat(sender.getQueue().size()).isEqualTo(3); // CREATE, UPDATE & OLD EVENT
    List<Object> events = sender.getQueue().peek(3);
    assertThat(events).hasSize(3);
    assertThat(((GatewaySenderEventImpl) events.get(0)).getValueAsString(true))
        .isEqualTo("Value_0");
    assertThat(((GatewaySenderEventImpl) events.get(1)).getValueAsString(true))
        .isEqualTo("Value_6");
    assertThat(((GatewaySenderEventImpl) events.get(2)).getValueAsString(true))
        .isEqualTo("Value_4");

    GatewaySenderEventImpl lastEvent = (GatewaySenderEventImpl) events.get(2);
    assertThat(lastEvent.getVersionTimeStamp()).isEqualTo(oldVersionTag.getVersionTimeStamp());
  }


  void saveLatestVersionTag(String regionName, String key) {
    InternalRegion internalRegion = (InternalRegion) cache.getRegion(regionName);
    oldVersionTag = ((EntrySnapshot) internalRegion.getEntry(key)).getVersionTag();
  }

  void enqueueEventWithOlderTimestamp(String senderName, List<Integer> remoteDSs, String regionName,
      String key, String value) {
    AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(senderName);

    InternalRegion internalRegion = (InternalRegion) cache.getRegion(regionName);

    InternalDistributedSystem ds = ((InternalCache) cache).getInternalDistributedSystem();
    EntryEventFactory eventFactory = new DefaultEntryEventFactory();
    EntryEventImpl event = eventFactory.create(internalRegion, Operation.UPDATE, key, value, null,
        false, ds.getDistributedMember());
    event.setTailKey(1L); // Without tailkey, the event is not processed by
                          // ParallelGatewaySenderEventProcessor.enqueueEvent

    event.setVersionTag(oldVersionTag);
    event.setEventId(new EventID(((InternalCache) cache).getInternalDistributedSystem()));
    sender.distribute(EnumListenerEvent.AFTER_UPDATE, event, remoteDSs, false);
  }
}
