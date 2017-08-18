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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Operation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventImpl;
import org.apache.geode.internal.cache.wan.GatewaySenderStats;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SerialGatewaySenderEventProcessorJUnitTest {

  private AbstractGatewaySender sender;

  private TestSerialGatewaySenderEventProcessor processor;

  @Before
  public void setUp() throws Exception {
    this.sender = mock(AbstractGatewaySender.class);
    this.processor = new TestSerialGatewaySenderEventProcessor(this.sender, "ny");
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

  private EventID handlePrimaryEvent() {
    GatewaySenderEventImpl gsei = mock(GatewaySenderEventImpl.class);
    EventID id = mock(EventID.class);
    when(gsei.getEventId()).thenReturn(id);
    this.processor.basicHandlePrimaryEvent(gsei);
    return id;
  }
}
