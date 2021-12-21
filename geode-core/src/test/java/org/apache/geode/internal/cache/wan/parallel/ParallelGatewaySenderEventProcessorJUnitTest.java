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
package org.apache.geode.internal.cache.wan.parallel;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.test.fake.Fakes;

public class ParallelGatewaySenderEventProcessorJUnitTest {

  private GemFireCacheImpl cache;
  private AbstractGatewaySender sender;

  @Before
  public void setUpGemFire() {
    createCache();
    createGatewaySender();
  }

  private void createCache() {
    // Mock cache
    cache = Fakes.cache();
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(cache.getDistributedSystem()).thenReturn(ids);
  }

  private void createGatewaySender() {
    // Mock gateway sender
    sender = ParallelGatewaySenderHelper.createGatewaySender(cache);
    when(sender.isBatchConflationEnabled()).thenReturn(true);
    when(sender.getStatistics()).thenReturn(mock(GatewaySenderStats.class));
  }

  @Test
  public void validateBatchConflationWithBatchContainingDuplicateConflatableEvents()
      throws Exception {
    // This tests normal batch conflation.

    // Create a ParallelGatewaySenderEventProcessor
    AbstractGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(sender);

    // Create a batch of conflatable events with duplicates
    List<GatewaySenderEventImpl> originalEvents = new ArrayList<>();
    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getFullPath()).thenReturn(SEPARATOR + "dataStoreRegion");
    when(lr.getCache()).thenReturn(cache);

    Object lastUpdateValue = "Object_13964_5";
    long lastUpdateSequenceId = 104, lastUpdateShadowKey = 28161;
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13964", "Object_13964_1", 100, 27709));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", "Object_13964_2", 101, 27822));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", "Object_13964_3", 102, 27935));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", "Object_13964_4", 103, 28048));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", lastUpdateValue, lastUpdateSequenceId, lastUpdateShadowKey));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.DESTROY,
        "Object_13964", null, 105, 28274));

    // Conflate the batch of events
    List<GatewaySenderEventImpl> conflatedEvents = processor.conflate(originalEvents);

    // Verify:
    // - the batch contains 3 events after conflation
    // - they are CREATE, UPDATE, and DESTROY
    // - the UPDATE event is the correct one
    assertThat(conflatedEvents.size()).isEqualTo(3);
    GatewaySenderEventImpl gsei1 = conflatedEvents.get(0);
    assertThat(gsei1.getOperation()).isEqualTo(Operation.CREATE);
    GatewaySenderEventImpl gsei2 = conflatedEvents.get(1);
    assertThat(gsei2.getOperation()).isEqualTo(Operation.UPDATE);
    GatewaySenderEventImpl gsei3 = conflatedEvents.get(2);
    assertThat(gsei3.getOperation()).isEqualTo(Operation.DESTROY);
    assertThat(gsei2.getDeserializedValue()).isEqualTo(lastUpdateValue);
    assertThat(gsei2.getEventId().getSequenceID()).isEqualTo(lastUpdateSequenceId);
    assertThat(gsei2.getShadowKey()).isEqualTo(lastUpdateShadowKey);
  }

  // See GEODE-7079: a NullPointerException was thrown whenever the queue was recovered from disk
  // and the processor started dispatching events before the actual region was available.
  @Test
  public void verifyBatchConflationWithNullEventRegionDoesNowThrowException()
      throws Exception {
    AbstractGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(sender);

    List<GatewaySenderEventImpl> events = new ArrayList<GatewaySenderEventImpl>();

    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getFullPath()).thenReturn(SEPARATOR + "dataStoreRegion");
    when(lr.getCache()).thenReturn(cache);

    // Create two events for the same key, so that conflation will be needed. Mock the getRegion()
    // value to return as null so we will hit the NPE if
    // it is referenced.
    GatewaySenderEventImpl gsei1 =
        spy(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
            "Object_13964", "Object_13964_1", 100, 27709));
    doReturn(null).when(gsei1).getRegion();

    GatewaySenderEventImpl gsei2 =
        spy(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.UPDATE,
            "Object_13964", "Object_13964_2", 101, 27822));
    doReturn(null).when(gsei2).getRegion();

    events.add(gsei1);
    events.add(gsei2);
    assertThatCode(() -> processor.conflate(events)).doesNotThrowAnyException();
  }

  @Test
  public void validateBatchConflationWithBatchContainingDuplicateNonConflatableEvents()
      throws Exception {
    // In certain cases, a batch could contain multiple CREATE events on the same region and key.
    // These events should not be conflated such that the order is incorrect.

    // Here is an example batch that contains 2 CREATE events on key=Object_14024:
    // SenderEventImpl[id=EventIDid=57bytes;threadID=0x10018|112;sequenceID=100;bucketId=24];operation=CREATE;region=/dataStoreRegion;key=Object_13964;shadowKey=27709]
    // SenderEventImpl[id=EventIDid=57bytes;threadID=0x10018|112;sequenceID=101;bucketId=24];operation=CREATE;region=/dataStoreRegion;key=Object_14024;shadowKey=27822]
    // SenderEventImpl[id=EventIDid=57bytes;threadID=0x10018|112;sequenceID=102;bucketId=24];operation=DESTROY;region=/dataStoreRegion;key=Object_13964;shadowKey=27935]
    // SenderEventImpl[id=EventIDid=57bytes;threadID=0x10018|112;sequenceID=104;bucketId=24];operation=CREATE;region=/dataStoreRegion;key=Object_14024;shadowKey=28161]

    // Create a ParallelGatewaySenderEventProcessor
    AbstractGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(sender);

    // Create a batch of non-conflatable events with one duplicate (not including the shadowKey)
    List<GatewaySenderEventImpl> originalEvents = new ArrayList<>();
    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getFullPath()).thenReturn(SEPARATOR + "dataStoreRegion");
    when(lr.getCache()).thenReturn(cache);

    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13964", "Object_13964", 100, 27709));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_14024", "Object_14024", 101, 27822));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.DESTROY,
        "Object_13964", null, 102, 27935));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_14024", "Object_14024", 104, 28161));

    // Conflate the batch of events
    List<GatewaySenderEventImpl> conflatedEvents = processor.conflate(originalEvents);

    // Assert no events were conflated incorrectly
    assertThat(originalEvents).isEqualTo(conflatedEvents);
  }

  @Test
  public void validateBatchConflationWithBatchContainingDuplicateConflatableAndNonConflatableEvents()
      throws Exception {
    // In certain cases, a batch could contain multiple equal conflatable events. These events
    // should not be conflated such that the order is incorrect.

    // Here is an example batch with multiple equal events on shadowKey=16813 and shadowKey=16700:
    // SenderEventImpl[id=EventID[threadID=104;sequenceID=2;bucketId=89];action=1;operation=UPDATE;region=/dataStoreRegion;key=Object_6079;shadowKey=16587]
    // SenderEventImpl[id=EventID[threadID=104;sequenceID=3;bucketId=89];action=2;operation=DESTROY;region=/dataStoreRegion;key=Object_6079;shadowKey=16700]
    // SenderEventImpl[id=EventID[threadID=112;sequenceID=9;bucketId=89];action=1;operation=PUTALL_UPDATE;region=/dataStoreRegion;key=Object_7731;shadowKey=16813]
    // SenderEventImpl[id=EventID[threadID=112;sequenceID=12;bucketId=89];action=1;operation=PUTALL_UPDATE;region=/dataStoreRegion;key=Object_6591;shadowKey=16926]
    // SenderEventImpl[id=EventID[threadID=104;sequenceID=3;bucketId=89];action=2;operation=DESTROY;region=/dataStoreRegion;key=Object_6079;shadowKey=16700]
    // SenderEventImpl[id=EventID[threadID=112;sequenceID=9;bucketId=89];action=1;operation=PUTALL_UPDATE;region=/dataStoreRegion;key=Object_7731;shadowKey=16813]

    // Create a ParallelGatewaySenderEventProcessor
    AbstractGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(sender);

    // Create mock region
    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getFullPath()).thenReturn(SEPARATOR + "dataStoreRegion");
    when(lr.getCache()).thenReturn(cache);

    // Create a batch of conflatable and non-conflatable events with one duplicate conflatable event
    // and one duplicate non-conflatable event (including the shadowKey)
    List<GatewaySenderEventImpl> originalEvents = new ArrayList<>();
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_6079", "Object_6079", 104, 2, 89, 16587));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.DESTROY,
        "Object_6079", null, 104, 3, 89, 16700));
    originalEvents
        .add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.PUTALL_UPDATE,
            "Object_7731", "Object_7731", 112, 9, 89, 16813));
    originalEvents
        .add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.PUTALL_UPDATE,
            "Object_6591", "Object_6591", 112, 12, 89, 16926));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.DESTROY,
        "Object_6079", null, 104, 3, 89, 16700));
    originalEvents
        .add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.PUTALL_UPDATE,
            "Object_7731", "Object_7731", 112, 9, 89, 16813));
    logEvents("original", originalEvents);

    // Conflate the batch of events
    List<GatewaySenderEventImpl> conflatedEvents = processor.conflate(originalEvents);
    logEvents("conflated", conflatedEvents);
    assertThat(conflatedEvents.size()).isEqualTo(4);
    assertThat(originalEvents.get(0)).isEqualTo(conflatedEvents.get(0));
    assertThat(originalEvents.get(1)).isEqualTo(conflatedEvents.get(1));
    assertThat(originalEvents.get(2)).isEqualTo(conflatedEvents.get(2));
    assertThat(originalEvents.get(3)).isEqualTo(conflatedEvents.get(3));
  }

  @Test
  public void validateBatchConflationWithDuplicateNonConflatableEvents()
      throws Exception {
    // Duplicate non-conflatable events should not be conflated.
    //
    // Here is an example batch with duplicate create and destroy events on the same key from
    // different threads:
    // GatewaySenderEventImpl[id=EventID[id=31bytes;threadID=0x30004|6;sequenceID=6072];operation=CREATE;region=/SESSIONS;key=6079],
    // GatewaySenderEventImpl[id=EventID[id=31bytes;threadID=0x30004|6;sequenceID=6073];operation=UPDATE;region=/SESSIONS;key=6079],
    // GatewaySenderEventImpl[id=EventID[id=31bytes;threadID=0x30004|5;sequenceID=6009];operation=CREATE;region=/SESSIONS;key=1736],
    // GatewaySenderEventImpl[id=EventID[id=31bytes;threadID=0x30004|6;sequenceID=6074];operation=DESTROY;region=/SESSIONS;key=6079],
    // GatewaySenderEventImpl[id=EventID[id=31bytes;threadID=0x30004|5;sequenceID=6011];operation=DESTROY;region=/SESSIONS;key=1736],
    // GatewaySenderEventImpl[id=EventID[id=31bytes;threadID=0x30004|6;sequenceID=6087];operation=CREATE;region=/SESSIONS;key=1736],
    // GatewaySenderEventImpl[id=EventID[id=31bytes;threadID=0x30004|6;sequenceID=6089];operation=DESTROY;region=/SESSIONS;key=1736],

    // Create a ParallelGatewaySenderEventProcessor
    AbstractGatewaySenderEventProcessor processor =
        ParallelGatewaySenderHelper.createParallelGatewaySenderEventProcessor(sender);

    // Create mock region
    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getFullPath()).thenReturn(SEPARATOR + "dataStoreRegion");
    when(lr.getCache()).thenReturn(cache);

    // Create a batch of conflatable events with duplicate create and destroy events on the same key
    // from different threads
    List<GatewaySenderEventImpl> originalEvents = new ArrayList<>();
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
        "6079", "6079", 6, 6072, 0, 0));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.UPDATE,
        "6079", "6079", 6, 6073, 0, 0));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
        "1736", "1736", 5, 6009, 0, 0));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.DESTROY,
        "6079", "6079", 6, 6074, 0, 0));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.DESTROY,
        "1736", "1736", 5, 6011, 0, 0));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.CREATE,
        "1736", "1736", 6, 6087, 0, 0));
    originalEvents.add(ParallelGatewaySenderHelper.createGatewaySenderEvent(lr, Operation.DESTROY,
        "1736", "1736", 6, 6089, 0, 0));
    logEvents("original", originalEvents);

    // Conflate the batch of events
    List<GatewaySenderEventImpl> conflatedEvents = processor.conflate(originalEvents);
    logEvents("conflated", conflatedEvents);

    // Assert no conflation occurs
    assertThat(conflatedEvents.size()).isEqualTo(7);
    assertThat(originalEvents).isEqualTo(conflatedEvents);
  }

  private void logEvents(String message, List<GatewaySenderEventImpl> events) {
    StringBuilder builder = new StringBuilder();
    builder.append("The list contains the following ").append(events.size()).append(" ")
        .append(message).append(" events:");
    for (GatewaySenderEventImpl event : events) {
      builder.append("\t\n").append(event.toSmallString());
    }
    System.out.println(builder);
  }
}
