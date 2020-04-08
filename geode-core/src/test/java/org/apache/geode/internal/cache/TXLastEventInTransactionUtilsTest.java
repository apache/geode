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

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.wan.GatewaySender;

public class TXLastEventInTransactionUtilsTest {

  public static final String SENDER_1 = "sender1";
  public static final String SENDER_2 = "sender2";
  public static final String SENDER_3 = "sender3";
  public static final String SENDER_4 = "sender4";
  public static final String SENDER_5 = "sender5";

  Cache cache;
  InternalRegion region1;
  InternalRegion region2;
  InternalRegion region3;
  InternalRegion region4;
  InternalRegion region5;
  InternalRegion region6;
  InternalRegion region7;
  InternalRegion region8;

  GatewaySender sender1;
  GatewaySender sender2;
  GatewaySender sender3;
  GatewaySender sender4;

  @Before
  public void setUp() {
    sender1 = mock(GatewaySender.class);
    when(sender1.isGroupTransactionEvents()).thenReturn(false);
    sender2 = mock(GatewaySender.class);
    when(sender2.isGroupTransactionEvents()).thenReturn(false);
    sender3 = mock(GatewaySender.class);
    when(sender3.isGroupTransactionEvents()).thenReturn(true);
    sender4 = mock(GatewaySender.class);
    when(sender4.isGroupTransactionEvents()).thenReturn(true);

    cache = mock(Cache.class);
    when(cache.getGatewaySender(SENDER_1)).thenReturn(sender1);
    when(cache.getGatewaySender(SENDER_2)).thenReturn(sender2);
    when(cache.getGatewaySender(SENDER_3)).thenReturn(sender3);
    when(cache.getGatewaySender(SENDER_4)).thenReturn(sender4);
    when(cache.getGatewaySender(SENDER_5)).thenReturn(null);

    final String listSenderIdsForRegion1_2[] = {SENDER_1, SENDER_2};
    final String listSenderIdsForRegion3_4[] = {SENDER_3, SENDER_4};
    final String listSenderIdsForRegion5_6[] = {SENDER_1, SENDER_3};
    final String listSenderIdsForRegion7[] = {SENDER_3};
    final String listSenderIdsForRegion8[] = {SENDER_5};


    Set senderIdsForRegion1_2 = new HashSet(Arrays.asList(listSenderIdsForRegion1_2));
    Set senderIdsForRegion3_4 = new HashSet(Arrays.asList(listSenderIdsForRegion3_4));
    Set senderIdsForRegion5_6 = new HashSet(Arrays.asList(listSenderIdsForRegion5_6));
    Set senderIdsForRegion7 = new HashSet(Arrays.asList(listSenderIdsForRegion7));
    Set senderIdsForRegion8 = new HashSet(Arrays.asList(listSenderIdsForRegion8));

    region1 = mock(InternalRegion.class);
    when(region1.getAllGatewaySenderIds()).thenReturn(senderIdsForRegion1_2);

    region2 = mock(InternalRegion.class);
    when(region2.getAllGatewaySenderIds()).thenReturn(senderIdsForRegion1_2);

    region3 = mock(InternalRegion.class);
    when(region3.getAllGatewaySenderIds()).thenReturn(senderIdsForRegion3_4);

    region4 = mock(InternalRegion.class);
    when(region4.getAllGatewaySenderIds()).thenReturn(senderIdsForRegion3_4);

    region5 = mock(InternalRegion.class);
    when(region5.getAllGatewaySenderIds()).thenReturn(senderIdsForRegion5_6);

    region6 = mock(InternalRegion.class);
    when(region6.getAllGatewaySenderIds()).thenReturn(senderIdsForRegion5_6);

    region7 = mock(InternalRegion.class);
    when(region7.getAllGatewaySenderIds()).thenReturn(senderIdsForRegion7);

    region8 = mock(InternalRegion.class);
    when(region8.getAllGatewaySenderIds()).thenReturn(senderIdsForRegion8);
  }

  @Test
  public void noSenderGroupingTransactions() throws Exception {
    List<EntryEventImpl> events = new ArrayList();
    EntryEventImpl event1 = createMockEntryEventImpl(region1);
    EntryEventImpl event2 = createMockEntryEventImpl(region2);

    events.add(event1);
    events.add(event2);

    EntryEventImpl lastTransactionEvent =
        TXLastEventInTransactionUtils.getLastTransactionEvents(events, cache);

    assertEquals(null, lastTransactionEvent);
  }

  @Test
  public void lastEventFoundAllSendersGroupTransactions() throws Exception {
    List<EntryEventImpl> events = new ArrayList();
    EntryEventImpl event1 = createMockEntryEventImpl(region3);
    EntryEventImpl event2 = createMockEntryEventImpl(region4);

    events.add(event1);
    events.add(event2);

    EntryEventImpl lastTransactionEvent =
        TXLastEventInTransactionUtils.getLastTransactionEvents(events, cache);

    assertEquals(event2, lastTransactionEvent);
  }

  @Test
  public void lastEventFoundNotAllSendersGroupTransactions() throws Exception {
    List<EntryEventImpl> events = new ArrayList();
    EntryEventImpl event1 = createMockEntryEventImpl(region5);
    EntryEventImpl event2 = createMockEntryEventImpl(region6);

    events.add(event1);
    events.add(event2);

    EntryEventImpl lastTransactionEvent =
        TXLastEventInTransactionUtils.getLastTransactionEvents(events, cache);

    assertEquals(event2, lastTransactionEvent);
  }

  @Test
  public void notAllEventsToSameGroupingSenders() throws Exception {
    List<EntryEventImpl> events = new ArrayList();
    EntryEventImpl event1 = createMockEntryEventImpl(region3);
    EntryEventImpl event2 = createMockEntryEventImpl(region7);

    events.add(event1);
    events.add(event2);

    assertThatThrownBy(() -> TXLastEventInTransactionUtils.getLastTransactionEvents(events, cache))
        .isInstanceOf(ServiceConfigurationError.class)
        .hasMessage("Not all events go to the same senders that group transactions");
  }

  @Test
  public void senderNotFound() throws Exception {
    List<EntryEventImpl> events = new ArrayList();
    EntryEventImpl event1 = createMockEntryEventImpl(region8);
    EntryEventImpl event2 = createMockEntryEventImpl(region8);

    events.add(event1);
    events.add(event2);

    assertThatThrownBy(() -> TXLastEventInTransactionUtils.getLastTransactionEvents(events, cache))
        .isInstanceOf(ServiceConfigurationError.class)
        .hasMessage("No information for senderId: sender5");
  }

  private EntryEventImpl createMockEntryEventImpl(InternalRegion region) {
    EntryEventImpl event = mock(EntryEventImpl.class);
    when(event.getRegion()).thenReturn(region);
    return event;
  }
}
