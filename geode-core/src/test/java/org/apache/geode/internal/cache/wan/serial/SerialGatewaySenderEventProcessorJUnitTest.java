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
package org.apache.geode.internal.cache.wan.serial;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.KeyInfo;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.fake.Fakes;

public class SerialGatewaySenderEventProcessorJUnitTest {

  private AbstractGatewaySender sender;

  private TestSerialGatewaySenderEventProcessor processor;

  private GemFireCacheImpl cache;

  private static final Logger logger = LogService.getLogger();

  @Before
  public void setUp() throws Exception {
    this.sender = mock(AbstractGatewaySender.class);
    this.processor =
        new TestSerialGatewaySenderEventProcessor(this.sender, "ny", null);
    this.cache = Fakes.cache();
    InternalDistributedSystem ids = mock(InternalDistributedSystem.class);
    when(this.cache.getDistributedSystem()).thenReturn(ids);
  }

  @Test
  public void validateUnprocessedTokensMapUpdated() throws Exception {
    GatewaySenderStats gss = mock(GatewaySenderStats.class);
    when(sender.getStatistics()).thenReturn(gss);

    // Handle primary event
    EventID id = handlePrimaryEvent();

    // Verify the token was added by checking the correct stat methods were called and the size of
    // the unprocessedTokensMap.
    verify(gss).incUnprocessedTokensAddedByPrimary();
    verify(gss, never()).incUnprocessedEventsRemovedByPrimary();
    assertEquals(1, this.processor.getUnprocessedTokensSize());

    // Handle the event from the secondary. The call to enqueueEvent is necessary to synchronize the
    // unprocessedEventsLock and prevent the assertion error in basicHandleSecondaryEvent.
    EntryEventImpl event = mock(EntryEventImpl.class);
    when(event.getRegion()).thenReturn(mock(LocalRegion.class));
    when(event.getEventId()).thenReturn(id);
    when(event.getOperation()).thenReturn(Operation.CREATE);
    this.processor.enqueueEvent(null, event, null);

    // Verify the token was removed by checking the correct stat methods were called and the size of
    // the unprocessedTokensMap.
    verify(gss).incUnprocessedTokensRemovedBySecondary();
    verify(gss, never()).incUnprocessedEventsAddedBySecondary();
    assertEquals(0, this.processor.getUnprocessedTokensSize());
  }

  @Test
  public void verifyUpdateVersionStampEventShouldNotBeAddToSecondary() throws Exception {
    // GatewaySenderStats gss = mock(GatewaySenderStats.class);
    // when(sender.getStatistics()).thenReturn(gss);
    // when(sender.isPrimary()).thenReturn(false);

    // // Handle primary event
    // EventID id = handlePrimaryEvent();

    // Verify the token was added by checking the correct stat methods were called and the size of
    // the unprocessedTokensMap.
    // verify(gss).incUnprocessedTokensAddedByPrimary();
    // verify(gss, never()).incUnprocessedEventsRemovedByPrimary();
    // assertEquals(1, this.processor.getUnprocessedTokensSize());

    // Handle the event from the secondary. The call to enqueueEvent is necessary to synchronize the
    // unprocessedEventsLock and prevent the assertion error in basicHandleSecondaryEvent.
    EntryEventImpl event = mock(EntryEventImpl.class);
    // when(event.getRegion()).thenReturn(mock(LocalRegion.class));
    // when(event.getEventId()).thenReturn(id);
    when(event.getOperation()).thenReturn(Operation.UPDATE_VERSION_STAMP);
    this.processor = spy(processor);
    this.processor.enqueueEvent(null, event, null);

    // Verify the token was removed by checking the correct stat methods were called and the size of
    // the unprocessedTokensMap.
    // verify(gss, never()).incUnprocessedEventsAddedBySecondary();
    // verify(gss).incUnprocessedTokensRemovedBySecondary();
    // verify(gss, never()).incUnprocessedEventsAddedBySecondary();
    verify(this.processor, never()).handleSecondaryEvent(any());
    // assertEquals(0, this.processor.getUnprocessedTokensSize());
  }

  @Test
  public void verifyCMENotOriginRemoteShouldNotBeAddToSecondary() throws Exception {
    EntryEventImpl event = mock(EntryEventImpl.class);
    when(event.getOperation()).thenReturn(Operation.CREATE);
    when(event.isConcurrencyConflict()).thenReturn(true);
    when(event.isOriginRemote()).thenReturn(false);
    this.processor = spy(processor);
    this.processor.enqueueEvent(null, event, null);
    verify(this.processor, never()).handleSecondaryEvent(any());
  }

  @Test
  public void verifyCMEButOriginRemoteShouldBeAddToSecondary() throws Exception {
    EntryEventImpl event = mock(EntryEventImpl.class);
    when(event.getOperation()).thenReturn(Operation.CREATE);
    when(event.isConcurrencyConflict()).thenReturn(true);
    when(event.isOriginRemote()).thenReturn(true);

    LocalRegion region = mock(LocalRegion.class);
    when(event.getRegion()).thenReturn(region);
    when(region.getFullPath()).thenReturn("/testRegion");

    EventID id = mock(EventID.class);
    when(event.getEventId()).thenReturn(id);

    this.processor = spy(processor);
    doNothing().when(processor).handleSecondaryEvent(any());

    this.processor.enqueueEvent(null, event, null);
    verify(this.processor, times(1)).handleSecondaryEvent(any());
  }

  @Test
  public void verifyNotCMENotUpdateVersionStampShouldBeAddToSecondary() throws Exception {
    EntryEventImpl event = mock(EntryEventImpl.class);
    when(event.getOperation()).thenReturn(Operation.CREATE);
    when(event.isConcurrencyConflict()).thenReturn(false);

    LocalRegion region = mock(LocalRegion.class);
    when(event.getRegion()).thenReturn(region);
    when(region.getFullPath()).thenReturn("/testRegion");

    EventID id = mock(EventID.class);
    when(event.getEventId()).thenReturn(id);

    this.processor = spy(processor);
    doNothing().when(processor).handleSecondaryEvent(any());

    this.processor.enqueueEvent(null, event, null);
    verify(this.processor, times(1)).handleSecondaryEvent(any());
  }

  @Test
  public void validateUnprocessedTokensMapReaping() throws Exception {
    // Set the token timeout low
    int originalTokenTimeout = AbstractGatewaySender.TOKEN_TIMEOUT;
    AbstractGatewaySender.TOKEN_TIMEOUT = 500;
    try {
      GatewaySenderStats gss = mock(GatewaySenderStats.class);
      when(sender.getStatistics()).thenReturn(gss);

      // Add REAP_THRESHOLD + 1 events to the unprocessed tokens map. This causes the uncheckedCount
      // in the reaper to be REAP_THRESHOLD. The next event will cause the reaper to run.\
      int numEvents = SerialGatewaySenderEventProcessor.REAP_THRESHOLD + 1;
      for (int i = 0; i < numEvents; i++) {
        handlePrimaryEvent();
      }
      assertEquals(numEvents, this.processor.getUnprocessedTokensSize());

      // Wait for the timeout
      Thread.sleep(AbstractGatewaySender.TOKEN_TIMEOUT + 1000);

      // Add one more event to the unprocessed tokens map. This will reap all of the previous
      // tokens.
      handlePrimaryEvent();
      assertEquals(1, this.processor.getUnprocessedTokensSize());
    } finally {
      AbstractGatewaySender.TOKEN_TIMEOUT = originalTokenTimeout;
    }
  }

  @Test
  public void validateUnProcessedEventsList() {
    Map<EventID, AbstractGatewaySender.EventWrapper> unprocessedEvents =
        (Map<EventID, AbstractGatewaySender.EventWrapper>) ReflectionTestUtils.getField(processor,
            "unprocessedEvents");

    long complexThreadId1 = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(0, 1, 1);
    long complexThreadId3 = ThreadIdentifier.createFakeThreadIDForParallelGSPrimaryBucket(0, 3, 3);
    unprocessedEvents.put(new EventID("mem1".getBytes(), complexThreadId1, 1L), null);
    unprocessedEvents.put(new EventID("mem2".getBytes(), 2L, 2L), null);

    String unProcessedEvents = this.processor.printUnprocessedEvents();
    logger.info("UnprocessedEvents: " + unProcessedEvents);
    assertThat(unProcessedEvents).contains("threadID=0x1010000|1;sequenceID=1");
    assertThat(unProcessedEvents).contains("threadID=2;sequenceID=2");

    processor.unprocessedTokens.put(new EventID("mem3".getBytes(), complexThreadId3, 3L), 3L);
    processor.unprocessedTokens.put(new EventID("mem4".getBytes(), 4L, 4L), 4L);
    String unProcessedTokens = this.processor.printUnprocessedTokens();
    logger.info("UnprocessedTokens: " + unProcessedTokens);
    assertThat(unProcessedTokens).contains("threadID=0x3010000|3;sequenceID=3");
    assertThat(unProcessedTokens).contains("threadID=4;sequenceID=4");
  }

  @Test
  public void validateBatchConflationWithDuplicateConflatableEvents()
      throws Exception {
    // This tests normal batch conflation

    // Create mock region
    List<GatewaySenderEventImpl> originalEvents = new ArrayList<>();
    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getFullPath()).thenReturn("/dataStoreRegion");
    when(lr.getCache()).thenReturn(this.cache);

    // Configure conflation
    when(this.sender.isBatchConflationEnabled()).thenReturn(true);
    when(sender.getStatistics()).thenReturn(mock(GatewaySenderStats.class));

    // Create a batch of conflatable events with duplicate update events
    Object lastUpdateValue = "Object_13964_5";
    long lastUpdateSequenceId = 104;
    originalEvents.add(createGatewaySenderEvent(lr, Operation.CREATE,
        "Object_13964", "Object_13964_1", 1, 100));
    originalEvents.add(createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", "Object_13964_2", 1, 101));
    originalEvents.add(createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", "Object_13964_3", 1, 102));
    originalEvents.add(createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", "Object_13964_4", 1, 103));
    originalEvents.add(createGatewaySenderEvent(lr, Operation.UPDATE,
        "Object_13964", lastUpdateValue, 1, lastUpdateSequenceId));
    originalEvents.add(createGatewaySenderEvent(lr, Operation.DESTROY,
        "Object_13964", null, 1, 105));

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

    // Create mock region
    LocalRegion lr = mock(LocalRegion.class);
    when(lr.getFullPath()).thenReturn("/dataStoreRegion");
    when(lr.getCache()).thenReturn(this.cache);

    // Configure conflation
    when(this.sender.isBatchConflationEnabled()).thenReturn(true);
    when(sender.getStatistics()).thenReturn(mock(GatewaySenderStats.class));

    // Create a batch of conflatable events with duplicate create and destroy events on the same key
    // from different threads
    List<GatewaySenderEventImpl> originalEvents = new ArrayList<>();
    originalEvents.add(createGatewaySenderEvent(lr, Operation.CREATE,
        "6079", "6079", 6, 6072));
    originalEvents.add(createGatewaySenderEvent(lr, Operation.UPDATE,
        "6079", "6079", 6, 6073));
    originalEvents.add(createGatewaySenderEvent(lr, Operation.CREATE,
        "1736", "1736", 5, 6009));
    originalEvents.add(createGatewaySenderEvent(lr, Operation.DESTROY,
        "6079", "6079", 6, 6074));
    originalEvents.add(createGatewaySenderEvent(lr, Operation.DESTROY,
        "1736", "1736", 5, 6011));
    originalEvents.add(createGatewaySenderEvent(lr, Operation.CREATE,
        "1736", "1736", 6, 6087));
    originalEvents.add(createGatewaySenderEvent(lr, Operation.DESTROY,
        "1736", "1736", 6, 6089));
    logEvents("original", originalEvents);

    // Conflate the batch of event
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

  private GatewaySenderEventImpl createGatewaySenderEvent(LocalRegion lr, Operation operation,
      Object key, Object value, long threadId, long sequenceId)
      throws Exception {
    when(lr.getKeyInfo(key, value, null)).thenReturn(new KeyInfo(key, null, null));
    EntryEventImpl eei = EntryEventImpl.create(lr, operation, key, value, null, false, null);
    eei.setEventId(new EventID(new byte[16], threadId, sequenceId));
    GatewaySenderEventImpl gsei =
        new GatewaySenderEventImpl(getEnumListenerEvent(operation), eei, null, true);
    return gsei;
  }

  private EnumListenerEvent getEnumListenerEvent(Operation operation) {
    EnumListenerEvent ele = null;
    if (operation.isCreate()) {
      ele = EnumListenerEvent.AFTER_CREATE;
    } else if (operation.isUpdate()) {
      ele = EnumListenerEvent.AFTER_UPDATE;
    } else if (operation.isDestroy()) {
      ele = EnumListenerEvent.AFTER_DESTROY;
    }
    return ele;
  }

  private EventID handlePrimaryEvent() {
    GatewaySenderEventImpl gsei = mock(GatewaySenderEventImpl.class);
    EventID id = mock(EventID.class);
    when(gsei.getEventId()).thenReturn(id);
    this.processor.basicHandlePrimaryEvent(gsei);
    return id;
  }
}
